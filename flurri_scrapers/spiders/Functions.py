import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
from random import randint


def get_events_for_this_venue(yelp_id):
    resourse = boto3.resource('dynamodb')
    table = resourse.Table('manual_add')

    filter_exp = Attr('yelp_id').eq(yelp_id)
    response = table.scan(FilterExpression=filter_exp)
    return response


def time_check(time_of_db_event, mode, time_of_scraped_event=None):
    current_time = datetime.now().date()
    db_beginning = datetime.strptime(time_of_db_event[0], '%Y %b %d').date()
    db_end = datetime.strptime(time_of_db_event[1], '%Y %b %d').date()

    if mode == "is past?":
        if db_end < current_time:
            return False
    if mode == "time match":
        sc_beginning = datetime.strptime(time_of_scraped_event[0], '%Y %b %d').date()
        sc_end = datetime.strptime(time_of_scraped_event[1], '%Y %b %d').date()
        if db_beginning == sc_beginning and db_end == sc_end:
            return True
        else:
            return False
    if mode == "overlap":
        sc_beginning = datetime.strptime(time_of_scraped_event[0], '%Y %b %d').date()
        sc_end = datetime.strptime(time_of_scraped_event[1], '%Y %b %d').date()
        if db_end >= sc_end > db_beginning:
            return True
        elif db_end > sc_beginning >= db_beginning:
            return True
        elif db_beginning == sc_beginning and db_end == sc_end:
            return True


#### debug !!! sc_beginning = datetime.strftime(time_of_scraped_event[0], '%Y %b %d').date()
# TypeError: descriptor 'strftime' requires a 'datetime.date' object but received a 'str'



def check_and_put_in_db(dbResponse, scrapedEvents):
    resourse = boto3.resource('dynamodb')
    table = resourse.Table('manual_add')
    databaseItems = dbResponse["Items"]
    updateTime = datetime.now().date()
    dbToRemove = []
    scToRemove = []

    # geting rid of past events for both db and scraped items
    for item in databaseItems:
        if time_check(item['schedule'].split(","), "is past?"):
            databaseItems.remove(item)

    for item in scrapedEvents:
        if time_check(item['schedule'].split(","), "is past?"):
            scrapedEvents.remove(item)

    # checking if any items need to be modified
    for item in databaseItems:
        for scraped_event in scrapedEvents:
            if time_check(item['schedule'].split(","), "time match",
                          time_of_scraped_event=scraped_event["schedule"].split(",")):
                if item["event_name"] == scraped_event["name"]:
                    table.update_item(
                        Key={
                            'id': str(item['id'])
                        },
                        UpdateExpression='SET event_name = :val1, updated = :val2, image_link = :val3, source_link = :val4, description = :val5',
                        ExpressionAttributeValues={
                            ':val1': scraped_event['name'],
                            ':val2': str(updateTime),
                            ':val3': scraped_event['image'],
                            ':val4': scraped_event['source_link'],
                            ':val5': scraped_event['description']
                        }
                    )

                    dbToRemove.append(item)
                    scToRemove.append(scraped_event)

    # removing updated items
    for item in dbToRemove:
        databaseItems.remove(item)

    for item in scToRemove:
        scrapedEvents.remove(item)

    # putting new items in db
    for scraped_event in scrapedEvents:
        table.put_item(
            Item={
                'id': str(randint(1000000000, 9999999999)),
                "category_rb": scraped_event["category_rb"],
                "content_type": scraped_event['content_type'],
                "content_subtype1": scraped_event['content_subtype1'],
                "content_subtype2": scraped_event['content_subtype2'],
                "event_name": scraped_event["name"],
                "schedule_type": scraped_event["schedule_type"],
                "schedule_except": scraped_event["schedule_except"],
                "schedule": scraped_event["schedule"],
                "address": scraped_event["address"],
                "start_time": scraped_event["start_time"],
                "end_time": scraped_event["end_time"],
                "source_link": scraped_event['source_link'],
                "tags": scraped_event["tags"],
                "image_link": scraped_event["image"],
                "artist": scraped_event["artist"],
                "yelp_id": scraped_event["yelp_id"],
                "description": scraped_event["description"],
                "mb_cancelled": False,
                "updated": str(updateTime)
            })

    # marking db items as "cancelled or changed"
    for item in databaseItems:
        table.update_item(
            Key={
                'id': str(item['id'])
            },
            UpdateExpression='SET mb_cancelled = :val1, updated = :val2',
            ExpressionAttributeValues={
                ':val1': True,
                ':val2': str(updateTime),
            }
        )
