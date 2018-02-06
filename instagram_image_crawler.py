# -*-encoding = utf-8 -*-
from instaLooter import InstaLooter
import csv
from datetime import datetime
import shutil
import os
import requests
from time import sleep
import pandas as pd


class InstaImageCrawler:
    HEADER_ARRAY = ['postid', 'username', 'userid', 'created_time', 'code', 'tags', 'img_url']
    TEMP_DIR = './temp'
    
    def __init__(self, tag_file='tags.txt', image_dir="./image", 
                 timelog_dir='./collected_time'):
        """
        tag_file : Path to the text file which contains tags to be crawled. 
                   Tags should be seperated by newline.
                   Default to 'tags.txt'
        image_dir : Folder where crawled image will be saved.
        timelog_dir : Folder where latest cralwed time is saved.
        lasttime_dict : Dict saving last timelog
        """
        self.tags = self.loadTag(tag_file)
        self.image_dir = image_dir
        self.timelog_dir = timelog_dir
        self.starttime_dict = {}
        self.temp_dataframe = pd.DataFrame(columns=self.HEADER_ARRAY)
        self.makeBase()
    
    def makeBase(self):
        if not os.path.exists(self.image_dir):
            os.mkdir(self.image_dir)
        if not os.path.exists(self.timelog_dir):
            os.mkdir(self.timelog_dir)
        if not os.path.exists(self.TEMP_DIR):
            os.mkdir(self.TEMP_DIR)
        if not os.path.exists(os.path.join(self.TEMP_DIR, 'images')):
            os.mkdir(os.path.join(self.TEMP_DIR, 'images'))
        for tag in self.tags:
            if not os.path.exists(os.path.join(self.timelog_dir, tag)):
                f = open(os.path.join(self.timelog_dir, tag), 'w')
                f.close()

    def loadTag(self, tag_file):
        """Load tags from tags.txt and returns list of tag"""
        tags = []
        tagText = open(tag_file, "r", encoding="utf-8")
        for tag in tagText:
            tags.append(tag.rstrip())
        tagText.close()
        return tags

    def crawlTag(self, tag, goal = 0):
        """
        Loads the last crawled time from timelog_dir, and
        crawl posts made after that time.

        Save the time when crawling started in starttime_dict
        
        Returns list of dict of posts.
        """

        with open(os.path.join(self.timelog_dir, tag), 'r') as time_log_file:
            last_crawled_timestamp = time_log_file.read().strip()
            try:
                last_crawled_timestamp = float(last_crawled_timestamp)
            except ValueError:
                last_crawled_timestamp = 0
 
        start_time = datetime.now()
        print("{} : {} starts".format(tag, str(start_time)))
        print("Last crawled date : {}\n".format(str(datetime.fromtimestamp(last_crawled_timestamp))))

        self.starttime_dict[tag] = start_time

        post_list = []

        looter = InstaLooter(hashtag=tag)
        count = 0

        for media in looter.medias():
            code = media['code']
            try:
                postDict = looter.get_post_info(code)
            except KeyError:
                continue
            except AttributeError:
                continue

            try:
                rowDict = self.makeRowDict(postDict)
            except IndexError:
                continue

            if not rowDict:
                continue
            
            print("{} / {}".format(postDict['date'], last_crawled_timestamp))
            if float(postDict['date']) <= last_crawled_timestamp:
                break

            post_list.append(rowDict)
            count += 1

            if (count % 500) == 0:
                print("{} : {} counts at {}\n".format(tag, str(count),
                                                      str(datetime.now())))
            # Finish Point
            if count == goal:
                break

        print("{} : {} ends \n".format(tag, str(datetime.now())))
        looter.__del__()

        return post_list

    def makeRowDict(self, postDict):
        """Make post as dict from raw Instagram json"""
        try:
            caption = postDict['edge_media_to_caption']['edges'][0]['node']['text']
        except IndexError:
            return False

        tag_all = self.findTag(caption)
        if tag_all == "":
            return False
        
        result = {}
        result['postid'] = postDict['id']
        result['username'] = postDict['owner']['username']
        result['userid'] = postDict['owner']['id']
        result['created_time'] = postDict['date']
        result['code'] = self.makeURL(postDict['code'])
        result['tags'] = tag_all
        result['img_url'] = postDict['display_src']

        return result

    def findTag(self, caption):
        """Find tags from given caption and return list of them."""
        tag = ""
        flag = False
        for ch in caption:
            if ch == "\n":
                continue
            if ch == "#":
                flag = True
            if flag:
                tag = tag + ch
            if ch == " ":
                flag = False
        return tag

    def makeURL(self, code):
        return 'https://www.instagram.com/p/{}/'.format(code)

    def downloadImage(self):
        """
        Download images from temp_dataframe and image_not_downloaded.pickle.
        Drop rows with no image.

        Returns dataframe with rows which images are not downloaded. (Those rows are also dropped.)
        """
        print('Downloading images...')
        try:
            not_downloaded_df = pd.read_pickle('./image_not_downloaded.pickle')
            self.temp_dataframe = self.temp_dataframe.append(not_downloaded_df, ignore_index=True)
        except:
            pass

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'}
        count = 0
        index_finished = []
        index_404 = []
        index_not_downloaded = []
        for row in self.temp_dataframe.itertuples():
            image_url = row.img_url
            postid = row.postid

            try:
                response = requests.get(url=image_url, headers=headers, stream=True)
            except ConnectionResetError as e:
                index_not_downloaded.append(row.index)
                continue
            if response.status_code == 200:
                with open(os.path.join(self.TEMP_DIR, 'images', '{}.{}'.format(postid, image_url[-3:])), 'wb') as image_file:
                    response.raw.decode_content = True
                    shutil.copyfileobj(response.raw, image_file)

                index_finished.append(row.index)
            elif response.status_code == 404:
                index_404.append(row.index)
                continue
            else:
                print(response)
                index_not_downloaded.append(row.index)
                continue
            count += 1
            if count % 500 == 0:
                print(count)
            sleep(0.7)

        df_not_downloaded = self.temp_dataframe.filter(axis=0, items=index_not_downloaded)
        df_not_downloaded = df_not_downloaded.reset_index(drop=True)
        self.temp_dataframe = self.temp_dataframe.drop(index_404 + index_not_downloaded)
        self.temp_dataframe = self.temp_dataframe.reset_index(drop=True)

        return df_not_downloaded

    def updateTimeLog(self, tag):
        with open(os.path.join(self.timelog_dir, tag), 'w') as time_log_file:
            time_log_file.write(str(int(self.starttime_dict[tag].timestamp())))
        print('Updated last crawled time of tag {} to {}'.format(tag, str(self.starttime_dict[tag])))

    def mergeTemp(self):
        print('Merging crawled data to the mainstream...')
        try:
            mainstream = pd.read_pickle('post_data.pickle')
            mainstream = mainstream.append(self.temp_dataframe, ignore_index=True)
        except OSError:
            mainstream = self.temp_dataframe

        mainstream.to_pickle('post_data.pickle')

        for image in os.scandir(os.path.join(self.TEMP_DIR, 'images')):
            try:
                shutil.move(image.path, self.image_dir)
            except shutil.Error:
                continue

    def merge_post_list_with_dataframe(self, post_list):
        post_list_dataframe = pd.DataFrame(post_list)
        self.temp_dataframe = self.temp_dataframe.append(post_list_dataframe, ignore_index=True)

    def filter_tags(self, tags):
        tags = ''.join(e for e in tags if (e.isalnum() or e == '#'))
        tags = tags.lstrip('#').split('#')
        return tags

    def crawl(self):
        for tag in self.tags:
            post_list = self.crawlTag(tag)
            self.merge_post_list_with_dataframe(post_list)

        self.temp_dataframe.drop_duplicates(subset='postid')
        self.temp_dataframe['tags'] = self.temp_dataframe['tags'].apply(self.filter_tags)
        df_not_downloaded = self.downloadImage()
        df_not_downloaded.to_pickle('./image_not_downloaded.pickle')

        for tag in self.tags:
            self.updateTimeLog(tag)
        self.mergeTemp()


if __name__ == "__main__":
    crawler = InstaImageCrawler()
    crawler.crawl()
