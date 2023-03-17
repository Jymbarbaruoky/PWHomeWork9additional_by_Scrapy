import json

import scrapy
from itemadapter import ItemAdapter
from scrapy.crawler import CrawlerProcess
from scrapy.item import Item, Field
from pymongo import MongoClient


myclient = MongoClient("mongodb+srv://PWHomeWork:123321@cluster0.duijelz.mongodb.net/PWHomeWork9-2?retryWrites=true&w=majority")
db = myclient["PWHomeWork9-2"]
collection_authors = db["authors"]
collection_quotes = db["quotes"]


class QuoteItem(Item):
    tags = Field()
    author = Field()
    quote = Field()


class AuthorItem(Item):
    fullname = Field()
    born_date = Field()
    born_location = Field()
    description = Field()


class QuotesPipeLine:
    quotes = []
    authors = []

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if 'fullname' in adapter.keys():
            self.authors.append({
                "fullname": adapter["fullname"],
                "born_date": adapter["born_date"],
                "born_location": adapter["born_location"],
                "description": adapter["description"]
            })
        if 'quote' in adapter.keys():
            self.quotes.append({
                "quote": adapter["quote"],
                "author": adapter["author"],
                "tags": adapter["tags"]
            })
        return item

    def close_spider(self, spider):
        with open('data/authors.json', 'w', encoding='utf-8') as f:
            json.dump(self.authors, f, ensure_ascii=False)
        collection_authors.insert_many(self.authors)
        with open('data/quotes.json', 'w', encoding='utf-8') as f:
            json.dump(self.quotes, f, ensure_ascii=False)
        collection_quotes.insert_many(self.quotes)


class QuotesSpider(scrapy.Spider):
    name = 'authors'
    allowed_domains = ['quotes.toscrape.com']
    start_urls = ['http://quotes.toscrape.com/']
    custom_settings = {"ITEM_PIPELINES": {QuotesPipeLine: 300}}

    def parse(self, response, *_):
        for quote in response.xpath("/html//div[@class='quote']"):
            tags = quote.xpath("div[@class='tags']/a/text()").extract(),
            author = quote.xpath("span/small/text()").get().strip(),
            q = quote.xpath("span[@class='text']/text()").get().strip()
            yield QuoteItem(tags=tags, author=author, quote=q)
            yield response.follow(url=self.start_urls[0] + quote.xpath('span/a/@href').get(),
                                  callback=self.nested_parse_author)
        next_link = response.xpath("//li[@class='next']/a/@href").get()
        if next_link:
            yield scrapy.Request(url=self.start_urls[0] + next_link)

    def nested_parse_author(self, response, *_):
        author = response.xpath('/html//div[@class="author-details"]')
        fullname = author.xpath('h3[@class="author-title"]/text()').get().strip()
        born_date = author.xpath('p/span[@class="author-born-date"]/text()').get().strip()
        born_location = author.xpath('p/span[@class="author-born-location"]/text()').get().strip()
        description = author.xpath('div[@class="author-description"]/text()').get().strip()
        yield AuthorItem(fullname=fullname, born_date=born_date, born_location=born_location, description=description)


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(QuotesSpider)
    process.start()
