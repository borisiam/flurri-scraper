import scrapy
from datetime import datetime
import re
from .Functions import get_events_for_this_venue, check_and_put_in_db


class BggSpider(scrapy.Spider):
    name = "berggruen"
    start_urls = ['http://www.berggruen.com/exhibitions']
    yelp_id = "berggruen-gallery-san-francisco"
    default_category = "Exhibition"
    tags = ["Art", "Contemporary", "Gallery"]
    venue_start = "10:am"
    venue_end = "6:pm"
    sched_type = "Long"
    content_type = "Art"
    content_subtype1 = "Contemporary"
    content_subtype2 = "Exhibit"
    venue_address = "10 Hawthorne St San Francisco, CA 94105"
    sched_except = "Sun"

    eventsList = []

    def parse(self, response):

        # initial soup

        events = response.css('#exhibitions-container')
        imglink = events.xpath('//div[@class="image"]/img/@src')
        artistnames = events.css('div.headers h1::text')
        exhnames = events.css('div.headers h2::text')
        schedule1 = events.css('div.headers h3::text')[0].extract()
        schedule2 = events.css('div.headers h3::text')[1].extract()
        source_link = events.xpath('//div[@id="exhibitions-container"]//div[@class="entry small"]/a/@href')

        # Artists

        artist1 = artistnames[0].extract()
        artist2 = artistnames[1].extract()

        # FORMATTING SCHEDULE FOR 1ST EVENT -----------------------------------------------------------------------

        if schedule1:

            schedule1 = re.split("– |, ", schedule1)
            date1 = []

            for item in schedule1:
                date1.append(item.strip("\n ").strip(" "))
            beginDate = datetime.strptime(date1[0] + " " + date1[2], '%B %d %Y').strftime("%Y %b %d")
            endDate = datetime.strptime(date1[1] + " " + date1[2], '%B %d %Y').strftime("%Y %b %d")

            dates1 = beginDate+","+endDate

            # getting a description

            # FORMING EVENT1 OBJECT  -----------------------------------------------------------------------------

            self.eventsList.append({"name": exhnames[0].extract(),
                                    "artist": artist1,
                                    "schedule": str(dates1),
                                    "schedule_type": self.sched_type,
                                    "schedule_except": self.sched_except,
                                    "image": imglink[0].extract(),
                                    "tags": str(["Art", "Contemporary", "Gallery", artist1]),
                                    "yelp_id": self.yelp_id,
                                    "category_rb": self.default_category,
                                    "start_time": self.venue_start,
                                    "end_time": self.venue_end,
                                    "address": self.venue_address,
                                    "content_type": self.content_type,
                                    "content_subtype1": self.content_subtype1,
                                    "content_subtype2": self.content_subtype2,
                                    "source_link": 'http://www.berggruen.com' + source_link[0].extract(),
                                    "description": description
                                    })

        # FORMATTING SCHEDULE FOR 2ND EVENT ----------------------------------------------------------------------

        if schedule2:

            schedule2 = re.split("– |, ", schedule2)
            date2 = []

            for item in schedule2:
                date2.append(item.strip("\n ").strip(" "))
            beginDate1 = datetime.strptime(date2[0] + " " + date2[2], '%B %d %Y').strftime("%Y %b %d")
            endDate1 = datetime.strptime(date2[1] + " " + date2[2], '%B %d %Y').strftime("%Y %b %d")

            dates2 = beginDate1+","+endDate1
            # getting a description

            # FORMING EVENT2 OBJECT  -----------------------------------------------------------------------------

            self.eventsList.append({"name": exhnames[1].extract(),
                                    "artist": artist2,
                                    "schedule": str(dates2),
                                    "schedule_type": self.sched_type,
                                    "schedule_except": self.sched_except,
                                    "image": imglink[1].extract(),
                                    "tags": str(["Art", "Contemporary", "Gallery", artist2]),
                                    "yelp_id": self.yelp_id,
                                    "category_rb": self.default_category,
                                    "start_time": self.venue_start,
                                    "end_time": self.venue_end,
                                    "address": self.venue_address,
                                    "content_type": self.content_type,
                                    "content_subtype1": self.content_subtype1,
                                    "content_subtype2": self.content_subtype2,
                                    "source_link": 'http://www.berggruen.com' + source_link[1].extract(),
                                    "description": "None"
                                    })


        # PUTTING OBJECTS TO DYNAMO-DB  -----------------------------------------------------------------------------

        dbResponse = get_events_for_this_venue(self.yelp_id)
        check_and_put_in_db(dbResponse, self.eventsList)