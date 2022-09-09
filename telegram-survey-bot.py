#!/usr/bin/env python
# pylint: disable=unused-argument, wrong-import-position
# This program is dedicated to the public domain under the CC0 license.

"""
Simple Bot to reply to Telegram messages.

First, a few handler functions are defined. Then, those functions are passed to
the Application and registered at their respective places.
Then, the bot is started and runs until we press Ctrl-C on the command line.
"""

import logging
import yaml
import time
import json
import uuid

from telegram import __version__ as TG_VER
try:
    from telegram import __version_info__
except ImportError:
    __version_info__ = (0, 0, 0, 0, 0)  # type: ignore[assignment]

if __version_info__ < (20, 0, 0, "alpha", 1):
    raise RuntimeError(
        f"This example is not compatible with your current PTB version {TG_VER}. To view the "
        f"{TG_VER} version of this example, "
        f"visit https://docs.python-telegram-bot.org/en/v{TG_VER}/examples.html"
    )
from telegram import ForceReply, Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (   
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    PicklePersistence,
    filters
)

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

import avro_helper

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

def fetch_configs():
    # fetches the configs from the available file
    with open(CONFIGS_FILE, 'r') as config_file:
        config = yaml.load(config_file, Loader=yaml.CLoader)

        return config

CONFIGS_FILE = './configs/configs.yaml'
CONFIGS = fetch_configs()

NAME,COMPANY,LOCATION,PIE,CONFIRM = range(5)

RESPONSE_TOPIC = 'responses'
RESPONDENT_TOPIC = 'respondents'


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        f"Hi there! Welcome to the Thanksgiving survey bot. " 
        f"Here are the commands available to you:\n"
        "/survey\n"
        "/results"
    )


####################################################################################
#                                                                                  #
#                                 SURVEY HANDLERS                                  #
#                                                                                  #
# Note: The commands being used as part of survey handlers are named based on the  #
#       state in which the command is executed. In the NAME state, we execute the  #
#       name_command and then prompt the user for the next question.               #
#                                                                                  #
####################################################################################

async def survey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # check if user has responded already
    if context.user_data and context.user_data.get('pie'):
        await update.message.reply_text(
            f"You already took this survey and said that {context.user_data['pie']} was your favorite pie.\n\n "
            "See the survey /results instead."
        )

        return ConversationHandler.END
    else:
        await update.message.reply_text(
            "Let's take a Thanksgiving survey!\n\n"
            "Enter the name you'd like to be stored with your survey response. "
            "Use /cancel to leave the survey at any time."
        )

        return NAME

async def name_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # capture and store name data
    name = update.message.text
    context.user_data['name'] = name

    # create a uuid
    context.user_data['uuid'] = uuid.uuid4().int

    await update.message.reply_text(
        "Enter the company that you work for or use /skip to go to the next question."
    )

    return COMPANY


async def company_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # capture and store company data
    company = update.message.text
    if company != '/skip':
        # update state
        context.user_data['company'] = company
    elif context.user_data.get('company'):
        # remove information
        del context.user_data['company']

    await update.message.reply_text(
        "Enter the state where you reside or use /skip to go to the next question."
    )

    return LOCATION


async def location_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # capture and store company data
    location = update.message.text
    if location != '/skip':
        # update state
        context.user_data['location'] = location
    elif context.user_data.get('location'):
        # remove information
        del context.user_data['location']

    reply_keyboard = [
            ["Pumpkin Pie"], 
            ["Pecan Pie"], 
            ["Apple Pie"], 
            ["Thanksgiving Leftover Pot Pie"],
            ["Other"]
        ]

    await update.message.reply_text(
        "Which Thanksgiving Pie is your favorite?",
        reply_markup=ReplyKeyboardMarkup(
            reply_keyboard, one_time_keyboard=True, input_field_placeholder="Favorite Pie?"
        )
    )

    return PIE


async def pie_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    survey_response = update.message.text

    # update state
    context.user_data['pie'] = survey_response

    # build up summary for confirmation
    summary = f"Your name is {context.user_data['name']}.\n"
    if context.user_data.get('company'):
        summary += f"You work for {context.user_data['company']}.\n"
    if context.user_data.get('location'):
        summary += f"You live in {context.user_data['location']}.\n"
    summary += f"And your favorite Thanksgiving pie is {survey_response}.\n\n"

    await update.message.reply_text(
        "Please confirm your survey responses.\n\n"
        f"{summary}"
        "Is this correct? Reply /y to do so or use /cancel to start over.", 
        reply_markup=ReplyKeyboardRemove()
    )

    return CONFIRM


async def confirm_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    result = send_kafka_data(context.user_data)

    if result == 0:
        await update.message.reply_text(
            "You've confirmed your survey response! And it has been sent. "
            "Use /results see the results."
        )
    else:
        if context.user_data.get('uuid'):
            del context.user_data['uuid']
        if context.user_data.get('name'):
            del context.user_data['name']
        if context.user_data.get('company'):
            del context.user_data['company']
        if context.user_data.get('location'):
            del context.user_data['location']
        if context.user_data.get('pie'):
            del context.user_data['pie']

        await update.message.reply_text(
            "Your result was not sent; please use /survey to re-take the survey."
        )

    return ConversationHandler.END


async def results_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        f"Fetching response data from Kafka..."
    )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels and ends the conversation."""
    if context.user_data.get('uuid'):
            del context.user_data['uuid']
    if context.user_data.get('name'):
        del context.user_data['name']
    if context.user_data.get('company'):
        del context.user_data['company']
    if context.user_data.get('location'):
        del context.user_data['location']
    if context.user_data.get('pie'):
        del context.user_data['pie']

    await update.message.reply_text(
        "Cancelled survey."
    )

    return ConversationHandler.END


def send_kafka_data(survey_data): 
    # 1. set up schema registry
    sr_conf = {
        'url': CONFIGS['schema-registry']['schema.registry.url'],
        'basic.auth.user.info': CONFIGS['schema-registry']['basic.auth.user.info']
    }
    schema_registry_client = SchemaRegistryClient(sr_conf)

    ts = int(time.time())

    # 2. set up respondent producer
    respondent_avro_serializer = AvroSerializer(
            schema_registry_client = schema_registry_client,
            schema_str = avro_helper.respondent_schema,
            to_dict = avro_helper.Respondent.respondent_to_dict
    )

    respondent_producer_conf = CONFIGS['kafka']
    respondent_producer_conf['value.serializer'] = respondent_avro_serializer
    producer = SerializingProducer(respondent_producer_conf)

    # 3. send respondent message
    uuid = survey_data.get('uuid')
    try:
        respondent = avro_helper.Respondent(
            survey_data.get('name'), 
            survey_data.get('company'), 
            survey_data.get('location')
        )

        uuid = hash((hash(respondent), uuid))
        k = str(uuid)
        logger.info('Publishing respondent message for key ' + str(k))
        producer.produce(RESPONDENT_TOPIC, key=k, value=respondent, timestamp=ts) 
        producer.poll()
        producer.flush()
    except Exception as e:
        print(str(e))
        logger.error('Got exception ' + str(e))
        return 1

    # 4. set up response producer
    response_avro_serializer = AvroSerializer(
            schema_registry_client = schema_registry_client,
            schema_str = avro_helper.response_schema,
            to_dict = avro_helper.Response.response_to_dict
    )

    response_producer_conf = CONFIGS['kafka']
    response_producer_conf['value.serializer'] = response_avro_serializer
    producer = SerializingProducer(response_producer_conf)

    # 5. send respondent message
    try:
        response = avro_helper.Response(
            uuid, 
            survey_data.get('pie')
        )

        k = str(uuid)
        logger.info('Publishing survey response message for key ' + str(k))
        producer.produce(RESPONSE_TOPIC, key=k, value=response, timestamp=ts) 
        producer.poll()
        producer.flush()
    except Exception as e:
        print(str(e))
        logger.error('Got exception ' + str(e))

        return 1

    return 0


def main() -> None:
    # create the application and pass in bot token
    persistence = PicklePersistence(filepath="./data/thanksgiving_pie_survey_bot.data")
    application = Application.builder().token(CONFIGS['telegram']['api-token']).persistence(persistence).build()

    # define conversation handlers
    survey_handler = ConversationHandler(
        entry_points=[CommandHandler("survey", survey_command)],
        states={
            NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, name_command),
                CommandHandler("cancel", cancel_command)
            ],
            COMPANY: [
                MessageHandler(filters.TEXT & (~filters.COMMAND | filters.Regex("^\/skip$")), company_command), 
                CommandHandler("cancel", cancel_command)
            ],
            LOCATION: [
                MessageHandler(filters.TEXT & (~filters.COMMAND | filters.Regex("^\/skip$")), location_command),
                CommandHandler("cancel", cancel_command)
            ],
            PIE: [
                MessageHandler(filters.Regex("^(Pumpkin Pie|Pecan Pie|Apple Pie|Thanksgiving Leftover Pot Pie|Other)$"), pie_command),
                CommandHandler("cancel", cancel_command)
            ],
            CONFIRM: [
                CommandHandler("y", confirm_command), 
                CommandHandler("n", cancel_command)
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel_command)],
    )

    # add handlers
    application.add_handler(survey_handler)
    application.add_handler(CommandHandler("results", results_command))
    application.add_handler(CommandHandler("start", start_command))

    # run the bot application
    application.run_polling()


if __name__ == "__main__":
    main()
