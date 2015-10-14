import scrapy

class BlogSpider(scrapy.Spider):
    name = 'blogspider'

    # IMDB: Top URL for Genres=Action. Has 30,000 movies.
    start_urls = ['http://www.imdb.com/search/title?genres=action&sort=moviemeter,asc&start=51&title_type=feature']
    #start_urls = ['http://www.imdb.com/chart/top']
    #start_urls = ['http://www.imdb.com/title/tt0120338/']
    #start_urls = ['http://www.imdb.com/title/tt0120338/?ref_=fn_al_tt_1']
    #start_urls = ['http://www.imdb.com']

    # Restrict domain crawler to just this list.
    allowed_domains = ['imdb.com']
    
    # Lets limit the number of pages to be crawled per second.
    rate = 1

    def __init__(self):
        self.download_delay = 1/float(self.rate)

    def parse(self, response):
        base_url = 'http://www.imdb.com'
        print ("Inside Parse => ")
        #for url in response.css('ul li a::attr("href")'):
        #for url in response.css('ul li a::attr("href").text()').re(r'.*title/.*'):
        for url in response.xpath('//td[@class="title"]/a/@href').extract():
            movie_url = base_url + str(url)
            print ("URL found => " + movie_url)

            yield scrapy.Request(movie_url, self.parse_titles)

    def parse_titles(self, response):
        print ("Inside parse_titles( )")
        movie_link = response.url
        movie_title = response.xpath("//span[@class='itemprop' and @itemprop='name']/text()").extract()[0]
        movie_html_source_code = response.body
        star_rating = response.xpath("//div[@class='star-box giga-star']/div[@class='titlePageSprite star-box-giga-star']/text()").extract()[0]
        release_year = response.xpath("//table[@id='title-overview-widget-layout']//h1[@class='header']/span[@class='nobr']/a/text()").extract()[0]
        release_date = response.xpath("//table[@id='title-overview-widget-layout']//div[@class='infobar']/span[@class='nobr']/a/text()").extract()[0]
        release_country = response.xpath("//table[@id='title-overview-widget-layout']//div[@class='infobar']/span[@class='nobr']/a/text()").extract()[1]

        director = response.xpath("//table[@id='title-overview-widget-layout']//div[@class='txt-block'][1]/a/span[@class='itemprop']/text()").extract()[0]
        
        print ("    Movie Title =>  " + movie_title)
        print ("    Movie URL =>  " + movie_link)
        print ("    Movie Star Rating =>  " + star_rating)
        
        # Print / write to file the entire HTML contents.
        #print ("HTML Source Code" + movie_html_source_code)

        #for post_title in response.css('div.entries > ul > li a::text').extract():
        #    print ("movie_title" + movie_title)
        #    print (response)
        #    yield {'title': post_title, 'movie_title': movie_title}
