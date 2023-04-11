#!venv/bin/python3
import datetime, json, os, asyncio, telebot, logging

from pymongo.mongo_client import MongoClient
from dateutil.relativedelta import relativedelta
from telebot.async_telebot import AsyncTeleBot
from dotenv import load_dotenv


class EmptyJsonClientError(Exception):
    pass


def json_response(dt_from, dt_upto, group_type):
    client = MongoClient(MONGODB_URL)

    mydb = client["dump"]
    mycol = mydb["workers"]

    first_date = datetime.datetime.fromisoformat(dt_from)
    last_date = datetime.datetime.fromisoformat(dt_upto)

    aggr = mycol.aggregate([
        {'$match':
             {'$and':
                  [{'dt': {'$gte': first_date}}, {'dt': {'$lte': last_date}}, ], },
         },
        {'$project':
            {
                "day_iso": {"$dateToString": {"format": "%Y-%m-%dT00:00:00", "date": "$dt"}},
                "month_iso": {"$dateToString": {"format": "%Y-%m-01T00:00:00", "date": "$dt"}},
                "hour_iso": {"$dateToString": {"format": "%Y-%m-%dT%H:00:00", "date": "$dt"}},
                "value": "$value",
            }
        },
        {'$group':
            {
                '_id': f"${group_type}_iso",
                'sum_value': {'$sum': "$value", }
            }
        },
        {'$sort': {'_id': 1}}

    ])

    added_date = TYPE_GROUPS[group_type]

    result = {"dataset": [], "labels": []}

    for a in aggr:
        d = datetime.datetime.fromisoformat(a['_id'])
        while first_date < d:
            result["labels"].append(first_date.isoformat())
            result["dataset"].append(0)

            first_date += added_date

        result["labels"].append(d.isoformat())
        result["dataset"].append(a["sum_value"])

        first_date += added_date

    while first_date <= last_date:
        result["labels"].append(first_date.isoformat())
        result["dataset"].append(0)

        first_date += added_date

    client.close()

    return json.dumps(result)


def validate_json(query):
    try:
        js_data = json.loads(query)
        if not js_data:
            raise EmptyJsonClientError

        dt_from = js_data["dt_from"]
        dt_upto = js_data["dt_upto"]
        group_type = js_data["group_type"]

    except json.JSONDecodeError:
        return 'Not json data. For example:\n%s' % EXAMPLE
    except EmptyJsonClientError:
        return 'Empty json data. For example:\n%s' % EXAMPLE
    except KeyError:
        return 'Invalid query. For example:\n%s' % EXAMPLE
    else:
        return json_response(dt_from, dt_upto, group_type)


load_dotenv()

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
MONGODB_URL = os.environ.get('MONGODB_URL')

TYPE_GROUPS = {'month': relativedelta(months=1), 'day': relativedelta(days=1), 'hour': relativedelta(hours=1)}
EXAMPLE = """\
{
   "dt_from": "2022-09-01T00:00:00",
   "dt_upto": "2022-12-31T23:59:00",
   "group_type": "month"
}
    """

bot = AsyncTeleBot(TELEGRAM_TOKEN)
logger = telebot.logger
telebot.logger.setLevel(logging.DEBUG)


@bot.message_handler(commands=['help', 'start'])
async def send_welcome(message):
    msg = """Hi there, I accept the request in json format and send the json response. 
Input query for example:\n%s""" % EXAMPLE
    await bot.reply_to(message, msg)


@bot.message_handler(func=lambda message: True)
async def echo_message(message):
    result = validate_json(message.text)
    await bot.reply_to(message, result)


if __name__ == '__main__':
    asyncio.run(bot.polling())
