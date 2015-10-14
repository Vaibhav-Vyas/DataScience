import scrapy
import time
import os

def writeToFile(folderPath, filename, stringToWrite):
    cacheCwd = os.getcwd()
    cdToPath(folderPath)

    with open(filename, 'w') as f:
        f.write(stringToWrite)
    f.close()

    # Restore
    cdToPath(cacheCwd)

# make dirs
def makeDirs(destDir):
    if not os.path.exists(destDir):
        os.makedirs(destDir)

def cdToPath(destPath):
    os.chdir(os.path.join(destPath, ''))  

class BlogSpider(scrapy.Spider):
    name = 'blogspider'

    # IMDB: Top URL for Genres=Action. Has 30,000 movies.
    start_urls = ['http://www.imdb.com/search/title?genres=action&sort=moviemeter,asc&start=251&title_type=feature']
    #start_urls = ['http://www.imdb.com/chart/top']
    #start_urls = ['http://www.imdb.com/title/tt0120338/']
    #start_urls = ['http://www.imdb.com/title/tt0120338/?ref_=fn_al_tt_1']
    #start_urls = ['http://www.imdb.com']

    # Restrict domain crawler to just this list.
    allowed_domains = ['imdb.com']
    
    # Lets limit the number of pages to be crawled per second.
    rate = 1
    
    # HTML Directory   
    htmlDirName = ''
    htmlDirPath = ''
    crawlCount = 0
    
    def __init__(self):
        self.download_delay = 1/float(self.rate)
       
        resultsDir = "results"
        scriptLaunchTime = time.strftime("%Y_%m_%d_%H-%M-%S")
        self.htmlDirName = "IMDB_HTML_Files_" + scriptLaunchTime
        filename = "testFile.txt"

        # Lets create results dir.
        currDir = os.path.dirname(os.path.realpath(__file__))
        makeDirs(resultsDir)
        cdToPath(resultsDir)

        makeDirs(self.htmlDirName)
        cdToPath(self.htmlDirName)

        self.htmlDirPath = os.getcwd()
        print ("IMDB Files directory = " + self.htmlDirPath)

        # Create dummy file to check if folder creation worked.
        writeToFile(self.htmlDirPath, filename, "Hello! I am creating this dummy file.\n" + scriptLaunchTime)

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
        self.crawlCount = self.crawlCount + 1
        print ("-------------------------Starting Crawling:" + str(self.crawlCount) + "-------------")
        print ("Inside parse_titles( )")
        movie_link = response.url
        movie_title = response.xpath("//span[@class='itemprop' and @itemprop='name']/text()").extract()[0]
        star_rating = response.xpath("//div[@class='star-box giga-star']/div[@class='titlePageSprite star-box-giga-star']/text()").extract()[0]
        release_year = response.xpath("//table[@id='title-overview-widget-layout']//h1[@class='header']/span[@class='nobr']/a/text()").extract()[0]
        release_date = response.xpath("//table[@id='title-overview-widget-layout']//div[@class='infobar']/span[@class='nobr']/a/text()").extract()[0]

        # This unicode str has \n prefix and suffix. Cleaning it.
        release_country = response.xpath("//table[@id='title-overview-widget-layout']//div[@class='infobar']/span[@class='nobr']/a/text()").extract()[1]
        release_country = release_country.replace('\n', ' ')
        
        director = response.xpath("//table[@id='title-overview-widget-layout']//div[@class='txt-block'][1]/a/span[@class='itemprop']/text()").extract()[0]
        stars_list = response.xpath("//table[@id='title-overview-widget-layout']//div[@class='txt-block'][3]//span[@class='itemprop']/text()").extract()
        genre_list = response.xpath("//table[@id='title-overview-widget-layout']//div[@class='infobar']//span[@class='itemprop' and @itemprop='genre']/text()").extract()
        
        # This unicode str has \n prefix and suffix. Cleaning it.
        time_duration = response.xpath("//table[@id='title-overview-widget-layout']//div[@class='infobar']/time/text()").extract()[0]
        time_duration = time_duration.replace('\n', ' ')
        movie_html_source_code = response.body
        
        print ("    Movie Title =>  " + movie_title)
        print ("    Movie URL =>  " + movie_link)
        print ("    Movie Star Rating =>  " + star_rating)
        print ("    Movie release_year =>  " + release_year)
        print ("    Movie release_date =>  " + release_date)
        print ("    Movie release_country =>  " + release_country)
        print ("    Movie Director =>  " + director)
        print ("    Movie Stars_list =>  " + ', '.join(stars_list))
        print ("    Movie Genre_list =>  " + ', '.join(genre_list))
        print ("    Movie time_duration =>  " + time_duration)


        # Lets store a local copy of this page.
        # Creating filename from the key present in the path
        start_link_key = movie_link.rfind('/', 0, len(movie_link))
        end_link_key = len(movie_link)
        # print ("(Start, End) = " + str(start_link_key) + " , " + str(end_link_key))

        if (start_link_key == (len(movie_link) - 1)):
            start_link_key = movie_link.rfind('/', 0, len(movie_link) - 1)
            end_link_key = len(movie_link) - 1

        start_link_key = start_link_key + 1
        movie_filename = movie_link[start_link_key : end_link_key]

        # print ("(Start, End) = " + str(start_link_key) + " , " + str(end_link_key))
        print ("    Movie movie_folder name =>  " + self.htmlDirPath)
        print ("    Movie movie_filename =>  " + movie_filename)

        # Store this page in local directory.
        writeToFile(self.htmlDirPath, movie_filename, movie_html_source_code)
        
        # Print / write to file the entire HTML contents.
        # print ("HTML Source Code" + movie_html_source_code)

        #for post_title in response.css('div.entries > ul > li a::text').extract():
        #    print ("movie_title" + movie_title)
        #    yield {'title': post_title, 'movie_title': movie_title}
        print ("-------------------------END Crawling:" + str(self.crawlCount) + "------------------")

