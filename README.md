# Thanksgiving Survey Telegram Bot

A base repository to set up a telegram bot that collects survey information for a single-question, Thanksgiving survey. This project was originally used in conjunction with a survey-analysis [stream processing use case recipe](https://developer.confluent.io/tutorials/survey-responses/ksql.html) on Confluent Developer, and it uses Confluent Cloud clusters and Schema Registry.

I encourage anyone to use this as a starting point for their own basic Telegram bot!

## Configurations

For your convenience, a config file has been created in the `./config` directory as `config.yaml.sample`. To get started, add your Telegram api-token, update the Confluent Cloud API Key and Secret parameters as well as the Schema Registry API Key and Secret. Also confirm that your bootstrap server is properly set. Without these, the Telegram bot will not be able to connect to your cluster.

## Running

For now, the Telegram bot has been set up to interact with the `survey-entries` topic on Confluent Cloud.

Run the Telegram bot with:

`python3 telegram_survey_bot.py`

And then interact with your bot by finding it on Telegram and chatting with it!