#!/usr/bin/env python

import time
import json
from enum import IntEnum

from telegram import ForceReply, Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (   
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters
)

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


from helpers import clients,logging
from classes.survey_entry import SurveyEntry

logger = logging.set_logging('telegram_survey_bot')
config = clients.config()

SURVEY_STATE = IntEnum('SurveyState', [
    'NAME',
    'COMPANY',
    'LOCATION',
    'RESPONSE',
    'CONFIRM'
])


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
    await update.message.reply_text(
        "Let's take a Thanksgiving survey!\n\n"
        "Enter the name you'd like to be stored with your survey response. "
        "Use /cancel to leave the survey at any time."
    )

    return SURVEY_STATE.NAME

async def name_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # capture and store name data
    context.user_data['name'] = update.message.text

    # get chat_id as user_id
    context.user_data['user_id'] = str(update.message.chat_id)

    await update.message.reply_text(
        "Enter the company that you work for or use /skip to go to the next question."
    )

    return SURVEY_STATE.COMPANY


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

    return SURVEY_STATE.LOCATION


async def location_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # capture and store company data
    location = update.message.text
    if location != '/skip':
        # update state
        context.user_data['location'] = location
    elif context.user_data.get('location'):
        # remove information
        del context.user_data['location']

    # TODO, this is where survey_id and question would be fetched
    context.user_data['survey_id'] = str(1)

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

    return SURVEY_STATE.RESPONSE


async def response_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    survey_response = update.message.text

    # update state
    context.user_data['response'] = survey_response

    # build up summary for confirmation
    summary = f"Your name is {context.user_data['name']}.\n"
    if context.user_data.get('company'):
        summary += f"You work for {context.user_data['company']}.\n"
    else:
        context.user_data['company'] = None

    if context.user_data.get('location'):
        summary += f"You live in {context.user_data['location']}.\n"
    else:
        context.user_data['location'] = None

    summary += f"And your favorite Thanksgiving pie is {survey_response}.\n\n"

    await update.message.reply_text(
        "Please confirm your survey responses.\n\n"
        f"{summary}"
        "Is this correct? Reply /y to confirm or use /cancel to start over.", 
        reply_markup=ReplyKeyboardRemove()
    )

    return SURVEY_STATE.CONFIRM


async def confirm_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        send_entry(context.user_data)

        await update.message.reply_text(
            "You've confirmed your survey response! And it has been sent. "
            "Use /results see the results."
        )
    except Exception as e:
        await update.message.reply_text(
            "Your result was not sent; please use /survey to re-take the survey."
        )
    finally:
        return ConversationHandler.END


async def results_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        f"Fetching response data from Kafka..."
    )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels and ends the conversation."""
    await update.message.reply_text(
        "Cancelled survey."
    )

    return ConversationHandler.END


def send_entry(entry): 
    # send survey entry message
    try:
        # set up Kafka producer for survey entries
        producer = clients.producer(clients.entry_serializer())
        value = SurveyEntry.dict_to_entry(entry)

        k = str(entry.get('survey_id'))
        logger.info("Publishing survey entry message for survey %s", k)
        producer.produce(config['topics']['survey-entries'], key=k, value=value) 
    except Exception as e:
        logger.error("Got exception %s", e)
        raise e
    finally:
        producer.poll()
        producer.flush()


async def post_init(application: Application) -> None:
    await application.bot.set_my_commands([
        ('survey', "Take current survey"),
        #('results', "See current survey results")
        ])


def main() -> None:
    # create the application and pass in bot token
    application = Application.builder().token(config['telegram']['api-token']).post_init(post_init).build()

    # define conversation handlers
    survey_handler = ConversationHandler(
        entry_points=[CommandHandler("survey", survey_command)],
        states={
            SURVEY_STATE.NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, name_command),
                CommandHandler("cancel", cancel_command)
            ],
            SURVEY_STATE.COMPANY: [
                MessageHandler(filters.TEXT & (~filters.COMMAND | filters.Regex("^\/skip$")), company_command), 
                CommandHandler("cancel", cancel_command)
            ],
            SURVEY_STATE.LOCATION: [
                MessageHandler(filters.TEXT & (~filters.COMMAND | filters.Regex("^\/skip$")), location_command),
                CommandHandler("cancel", cancel_command)
            ],
            SURVEY_STATE.RESPONSE: [
                MessageHandler(filters.Regex("^(Pumpkin Pie|Pecan Pie|Apple Pie|Thanksgiving Leftover Pot Pie|Other)$"), 
                    response_command),
                CommandHandler("cancel", cancel_command)
            ],
            SURVEY_STATE.CONFIRM: [
                CommandHandler("y", confirm_command), 
                CommandHandler("n", cancel_command)
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel_command)],
    )

    # add handlers
    application.add_handler(survey_handler)
    #application.add_handler(CommandHandler("results", results_command))
    application.add_handler(CommandHandler("start", start_command))

    # run the bot application
    application.run_polling()


if __name__ == "__main__":
    main()
