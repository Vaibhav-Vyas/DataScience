import scrapy
import time
import os
import unicodedata

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
    start_urls = ['http://www.imdb.com/search/title?genres=action&sort=moviemeter,asc&start=51&title_type=feature']
    #start_urls = ['http://www.imdb.com/chart/top']
    #start_urls = ['http://www.imdb.com/title/tt0120338/']
    #start_urls = ['http://www.imdb.com/title/tt0120338/?ref_=fn_al_tt_1']
    #start_urls = ['http://www.imdb.com']

    # Restrict domain crawler to just this list.
    allowed_domains = ['imdb.com']
    
    # Lets limit the number of pages to be crawled per second.
    rate = 0.25
    
    # HTML Directory   
    htmlDirName = ''
    htmlDirPath = ''
    csvFilename = ''
    csvFp = 0
    jsonFp = 0
    recordHeader = ''
    crawlCount = 0

    def openCsvFile(self, folderPath, filename, headerToWrite):
        cacheCwd = os.getcwd()
        cdToPath(folderPath)

        self.csvFp = open(filename, 'w')
        self.csvFp.write(headerToWrite)
        # Restore
        cdToPath(cacheCwd)

    def openJsonFile(self, folderPath, filename, headerToWrite):
        cacheCwd = os.getcwd()
        cdToPath(folderPath)

        self.jsonFp = open(filename, 'w')
        self.jsonFp.write(headerToWrite)
        # Restore
        cdToPath(cacheCwd)

    def appendRecordToFile(self, filePtr, stringToWrite):
        filePtr.write(stringToWrite)
        
    def closeCsvFile(self):
        self.csvFp.close()

    def closeJsonFile(self):
        self.jsonFp.close()

    def __init__(self):
        self.download_delay = 1/float(self.rate)
       
        resultsDir = "results"
        scriptLaunchTime = time.strftime("%Y_%m_%d_%H-%M-%S")
        self.htmlDirName = "IMDB_HTML_Files_" + scriptLaunchTime
        filename = "testFile.txt"
        self.csvFilename = "result_allMoviesParsed_IMDB.csv"
        self.jsonFilename = "result_allMoviesParsed_IMDB_Json.json"

        self.recordHeader = 'record_id, movie_title, movie_url, movie_star_rating,release_year, release_date, release_country, director, stars_list, genre_list, time_duration, movie_filename, movie_folder name\n'
        headerToWrite = 'IMDB Movie retrieval results \n\n'
        
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

        self.openCsvFile(self.htmlDirPath,self.csvFilename, headerToWrite)
        self.openJsonFile(self.htmlDirPath,self.jsonFilename, ' ')

        self.appendRecordToFile(self.csvFp, self.recordHeader)
        
        for i in range(1, 150):
            index = i * 50 + 1
            self.start_urls.append('http://www.imdb.com/search/title?genres=action&sort=moviemeter,asc&start=' + str(index) + '&title_type=feature')
        

    def parse(self, response):
        base_url = 'http://www.imdb.com'
        print ("Inside Parse => ")
        #for url in response.css('ul li a::attr("href")'):
        #for url in response.css('ul li a::attr("href").text()').re(r'.*title/.*'):
        
        # Let us get the link to next search page showing 250 rows.
        url = response.xpath("//div[@id='right']/span[@class='pagination']/a[2]/@href").extract()[1]
        movie_url = base_url + str(url)
        print ("LIST Page URL => " + movie_url)

        yield scrapy.Request(movie_url, self.parse_each_search_list_pg)

        # Passing this next search page, having 250 entries, to this same function.
        # Real use case of recursion :) :) 
        yield scrapy.Request(movie_url, self.parse)

    # This function retrieve each of the 250 entries and pass each page to 
    def parse_each_search_list_pg(self, response):
        base_url = 'http://www.imdb.com'
        print ("Inside Parse => ")
        #for url in response.css('ul li a::attr("href")'):
        #for url in response.css('ul li a::attr("href").text()').re(r'.*title/.*'):
        for url in response.xpath('//td[@class="title"]/a/@href').extract():
            movie_url = base_url + str(url)
            print ("URL found => " + movie_url)
            yield scrapy.Request(movie_url, self.parse_movies)

    def unicode_to_ascii(self, strInput):
        strInput = unicodedata.normalize('NFKD', strInput).encode('ascii','ignore')
        strInput.strip()
        return strInput
    
    def parse_movies(self, response):
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
        
        
        #movie_link = self.unicode_to_ascii(movie_link)
        #movie_title = self.unicode_to_ascii(movie_title)
        #star_rating = self.unicode_to_ascii(star_rating)
        #release_year = self.unicode_to_ascii(release_year)
        #release_date = self.unicode_to_ascii(release_date)
        release_country = self.unicode_to_ascii(release_country)
        #director = self.unicode_to_ascii(director)
        #stars_list = self.unicode_to_ascii(stars_list)
        #genre_list = self.unicode_to_ascii(genre_list)
        time_duration = self.unicode_to_ascii(time_duration)

        # Lets store a local copy of this page.
        # Creating filename from the key present in the path
        start_link_key = movie_link.rfind('/', 0, len(movie_link))
        end_link_key = len(movie_link)
        # print ("(Start, End) = " + str(start_link_key) + " , " + str(end_link_key))

        if (start_link_key == (len(movie_link) - 1)):
            start_link_key = movie_link.rfind('/', 0, len(movie_link) - 1)
            end_link_key = len(movie_link) - 1

        start_link_key = start_link_key + 1
        movie_record_pri_key = movie_link[start_link_key : end_link_key]
        movie_filename = movie_record_pri_key + ".html"

        # print ("(Start, End) = " + str(start_link_key) + " , " + str(end_link_key))
        print ("    Movie primary key =>  " + movie_record_pri_key)
        print ("    Movie Title =>  " + movie_title)
        print ("    Movie URL =>  " + movie_link)
        print ("    Movie Star Rating =>  " + star_rating)
        print ("    Movie release_year =>  " + release_year)
        print ("    Movie release_date =>  " + release_date)
        print ("    Movie release_country =>  " + release_country)
        print ("    Movie Director =>  " + director)
        print ("    Movie Stars_list =>  " + '; '.join(stars_list))
        print ("    Movie Genre_list =>  " + '; '.join(genre_list))
        print ("    Movie time_duration =>  " + time_duration)
        print ("    Movie movie_filename =>  " + movie_filename)
        print ("    Movie movie_folder name =>  " + self.htmlDirPath)

        # Store this page in local directory.
        writeToFile(self.htmlDirPath, movie_filename, movie_html_source_code)
        
        # Print / write to file the entire HTML contents.
        # print ("HTML Source Code" + movie_html_source_code)

        #for post_title in response.css('div.entries > ul > li a::text').extract():
        #    print ("movie_title" + movie_title)

        yield {'record_id' : movie_record_pri_key, 'movie_title' : movie_title, 'movie_url' : movie_link,'movie_star_rating' : star_rating, 'release_year' : release_year, 'release_date' : release_date, 'release_country' : release_country, 'director' : director, 'stars_list' : '; '.join(stars_list), 'genre_list' : '; '.join(genre_list), 'time_duration' : time_duration, 'movie_filename' : movie_filename, 'movie_folder name' : self.htmlDirPath}
        
        
        jsonRecord = {'record_id' : movie_record_pri_key, 'movie_title' : movie_title, 'movie_url' : movie_link,'movie_star_rating' : star_rating, 'release_year' : release_year, 'release_date' : release_date, 'release_country' : release_country, 'director' : director, 'stars_list' : '; '.join(stars_list), 'genre_list' : '; '.join(genre_list), 'time_duration' : time_duration, 'movie_filename' : movie_filename, 'movie_folder name' : self.htmlDirPath}
        # Convert Unicode to Ascii
        #jsonRecord = unicodedata.normalize('NFKD', jsonRecord).encode('ascii','ignore')

        currMovieAsCsv = movie_record_pri_key + ',' + movie_title + ',' + movie_link + ',' + star_rating + ',' + release_year + ',' + release_date + ',' + release_country + ',' + director + ',' + '; '.join(stars_list) + ',' + '; '.join(genre_list) + ',' + time_duration + ',' + movie_filename + ',' + self.htmlDirPath + '\n'
        # Convert Unicode to Ascii
        currMovieAsCsv = unicodedata.normalize('NFKD', currMovieAsCsv).encode('ascii','ignore')

        self.appendRecordToFile(self.csvFp, currMovieAsCsv)
        self.appendRecordToFile(self.jsonFp, str(jsonRecord).encode('utf8'))
        self.appendRecordToFile(self.jsonFp, '\n')

        print ("-------------------------END Crawling:" + str(self.crawlCount) + "------------------")

