#!/usr/bin/env python2
# -*- coding: utf-8 -*-
# Copyright (C) 2013 - Remy van Elst

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from bs4 import BeautifulSoup
import os, json, sys, cgi, csv, urllib2, datetime, base64, re, urlparse, hashlib, urllib, HTMLParser
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

# Max pages from marktplaats to parse
max_pages = 2
# Max ads on new overview page
max_page_items = 100

pool = ThreadPool() 

base_url = "http://www.marktplaats.nl/z/verzamelen/spoorwegen-en-tramwegen.html?categoryId=944&sortBy=SortIndex&sortOrder=decreasing&currentPage="
title = "Spoorwegen"

filter = []

with open("./filter.txt") as filterfile:
    for line in filterfile:
        filter.append(line.rstrip('\n'))
num = 0

def create_folder(directory):
    """Creates directory if not exists yet"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def uniqify(seq):
    """Removes duplicates from a list and keeps order"""
    seen = set()
    seen_add = seen.add
    return [ x for x in seq if x not in seen and not seen_add(x)]

def page_to_soup(page, number=0):
    """Uses urllib2 to get a webpage and returns a beautifulsoup object of the html. If page number is given this is appended at end of url."""
    if number >= 1:
        page_url = page + str(number)
    else:
        page_url = page
    req = urllib2.Request(page_url, headers={'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/4.0; InfoPath.2; SV1; .NET CLR 2.0.50727; WOW64)'})
    try:
        page = urllib2.urlopen(req).read()
    except urllib2.HTTPError as e:
        print("Could not retreive HTTP page %s" % page_url)
        sys.exit(1) 
    page_soup = BeautifulSoup(page)
    return page_soup

def remove_double_whitespace(string):
    """Replaces double whitespaces with a space and newlines with a double space"""
    return re.sub(' +', ' ', re.sub('\n', '  ', str(string)))

def save_image(url, filename):
    """Saves image from url to filename and returns filename"""
    urllib.urlretrieve(url, filename)
    return filename

def parse_overview_page(page_soup):
    """Parses a Marktplaats.nl advertisement category overview page and returns an dict with data."""
    ads = []
    for item in page_soup.find_all(attrs={'class': "defaultSnippet"}):
        stop_loop = False
        item_soup = BeautifulSoup(item.encode('utf-8'))
        item_title = item_soup.find(attrs={'class':'mp-listing-title'}).get_text().encode('utf-8')
        item_descr = item_soup.find(attrs={'class':'mp-listing-description'})
        if item_descr:
            item_descr = item_soup.find(attrs={'class':'mp-listing-description'}).get_text().encode('utf-8')
        item_descr_ext = item_soup.find(attrs={'class':'mp-listing-description-extended'})
        if item_descr_ext:
            item_descr_ext = item_descr_ext.get_text().encode('utf-8')
        item_price = item_soup.find(attrs={'class':'column-price'}).get_text().encode('utf-8')
        item_location = item_soup.find(attrs={'class':'location-name'}).get_text().encode('utf-8')
        item_seller = item_soup.find(attrs={'class':'seller-name'}).get_text().encode('utf-8')
        item_attrs = item_soup.find(attrs={'class':'mp-listing-title'}).get_text().encode('utf-8')
        item_date = item_soup.find(attrs={'class':'column-date'}).get_text().encode('utf-8')
        item_url = ""
        seller_url = ""
        for link in item_soup.find_all('a'):
            parse = urlparse.urlparse(link.get('href'))
            if parse.netloc == "www.marktplaats.nl" and str(parse.path).startswith("/a/"):
                item_url = link.get('href')
            if parse.netloc == "www.marktplaats.nl" and str(parse.path).startswith("/verkopers/"):
                seller_url = link.get('href')
        item_prio = item_soup.find(attrs={'class':'mp-listing-priority-product'})
        if item_prio:
            item_prio = item_prio.get_text().encode('utf-8')
        item_img_src = item_soup.img['src']
        if remove_double_whitespace(item_prio) == "Topadvertentie":
            stop_loop = True
            print("Filtering out sponsored ad.")
        for filteritem in filter:
            if filteritem.lower() in str(remove_double_whitespace(item_title)).lower() or filteritem in str(remove_double_whitespace(item_descr_ext)).lower() or filteritem in str(remove_double_whitespace(item_descr)).lower():
                stop_loop = True
                print("Filtering out ad with word trigger %s." % filteritem.lower())
        if not stop_loop:
            ad_data = {}
            ad_data["title"] = remove_double_whitespace(item_title)
            ad_data["descr"] = remove_double_whitespace(item_descr)
            ad_data["descr_extra"] = remove_double_whitespace(item_descr_ext)
            ad_data["price"] = remove_double_whitespace(item_price)
            ad_data["location"] = remove_double_whitespace(item_location)
            ad_data["seller"] = remove_double_whitespace(item_seller)
            ad_data["attrs"] = remove_double_whitespace(item_attrs)
            ad_data["date"] = remove_double_whitespace(item_date)
            ad_data["item_url"] = item_url
            ad_data["seller_url"] = seller_url
            ad_data["prio"] = remove_double_whitespace(item_prio)
            ad_data["img_url"] = "http:" + item_img_src
            stuff_to_hash = str(ad_data["title"] + ad_data["seller"] + ad_data["location"] + ad_data["price"])
            hash_object = hashlib.sha1(stuff_to_hash)
            hex_dig = hash_object.hexdigest()
            ad_data["uid"] = str(hex_dig)
            print("Parsing ad %s" % str(hex_dig))
            ads.append(ad_data)
    return ads

def create_ad_overview_json_file(ads_list):
    """Creates an json file for every ad in the ads list in its uid folder."""
    for ads in ads_list:
        for ad in ads:
            json_ad = json.dumps(ad)
            create_folder("pages/" + ad["uid"]) 
            with open("pages/" + ad["uid"] + "/overview_page.json", "wb") as file:
                file.write(json_ad)

def create_overview_page(ads_list, page_number, max_pages, filename):
    """Creates an overview page from a list with ad json data."""
    prev_pagination = []
    if page_number > 1:
        for prev_page_num in range(1, page_number):
            prev_pagination.append(("<li><a href='%s'>%s</a></li>" ) % (filename + "-" + str(prev_page_num) + ".html", str(prev_page_num)))
    next_pagination = []
    if page_number != max_pages:
        for next_page_num in range(page_number + 1, max_pages + 1):
            next_pagination.append(("<li><a href='%s'>%s</a></li>" ) % (filename + "-" + str(next_page_num) + ".html", str(next_page_num)))
    current_pagination = []
    current_pagination.append("<li class='active'><a href='#'>%s<span class='sr-only'>(current)</span></a></li>" % str(page_number))

    if page_number > 1:
        prev_filename = filename + "-" + str(page_number - 1) + ".html"
        prev_html = ("<a href='%s' class='btn btn-success btn-large'><i class='icon-white icon-arrow-left'></i>Previous Page</a>" % prev_filename)
    else:
        prev_filename = "#"
        prev_html = ("<button class='btn btn-sucess btn-large disabled'><i class='icon-white icon-arrow-left'></i>Previous Page</button>")
    if page_number == max_pages:
        next_filename = "#"
        next_html = ("<button class='btn btn-sucess btn-large disabled'><i class='icon-white icon-arrow-left'></i>Next Page</button>")
    else: 
        next_filename = filename + "-" + str(page_number + 1) + ".html"
        next_html = ("<a href='%s' class='btn btn-success btn-large'><i class='icon-white icon-arrow-right'></i>Next Page</a>" % next_filename)
    filename = filename + "-" + str(page_number) + ".html"

    with open(filename, "w") as file:
        file.write("<!DOCTYPE html><html lang='en'><head><title>Overview</title><link href='http://netdna.bootstrapcdn.com/bootswatch/3.1.1/cosmo/bootstrap.min.css' rel='stylesheet'><meta http-equiv='Content-Type' content='text/html; charset=UTF-8' /></head>")
        file.write('<body><a id="top-of-page"></a>')
        file.write('<div class="container-fluid ">')
        file.write('<div class="row"><div class="col-md-12">')
        file.write(("<h1>%s</h1>") % ( title ))
        file.write('<h6>Overview page #%s</h6>' % str(page_number))
        file.write("</div></div>")
        file.write("<div class='row'><div class='col-md-1'>")
        file.write(prev_html)
        file.write("</div><div class='col-md-10'><ul class='pagination' style='float: none;margin: 0 auto;'>")
        for line in prev_pagination:
            file.write(line)
        for line in current_pagination:
            file.write(line)
        for line in next_pagination:
            file.write(line)
        file.write("</ul></div><div class='col-md-1'>")
        file.write(next_html)
        file.write("</div></div>")
        file.write("<div class='row'><div class='col-md-12'>")
        file.write("<table class='table table-striped'>")
        file.write("<thead><tr>")
        file.write("<td>Foto</td>")
        file.write("<td>Info</td>")
        file.write("<td>Verkoper</td>")
        file.write("<td>Prijs</td>")
        file.write("</tr></thead><tbody>\n")
        for ad in ads_list:
           file.write("<tr>")
           file.write("<td><a href='")
           file.write("pages/" + str(ad["uid"]) + "/'><img src='")
           img_loc = "images/" + str(ad["uid"]) + '.jpg'
           if not os.path.exists("images/" + str(ad["uid"]) + '.jpg'):
               save_image(ad["img_url"], img_loc)
           file.write(img_loc)
           file.write("' style='width: 150px; height: auto; border:0;' alt='image' /></a></td>")
           file.write("<td><strong><a href='")
           file.write("pages/" + str(ad["uid"]) + "/index.html")
           file.write("'>")
           file.write(ad["title"].encode('ascii', 'xmlcharrefreplace'))
           file.write("</a></strong><br />")
           file.write(ad["descr"].encode('ascii', 'xmlcharrefreplace'))
           file.write("</td>")
           file.write("<td><a href='")
           file.write(ad["seller_url"])
           file.write("'>")
           file.write(ad["seller"].encode('ascii', 'xmlcharrefreplace'))
           file.write(" | ")
           file.write(ad["location"].encode('ascii', 'xmlcharrefreplace'))
           file.write("</a></td>")
           file.write("<td>")
           file.write("<strong>")
           file.write(ad["price"].encode('ascii', 'xmlcharrefreplace'))
           file.write("</strong></td>")
           file.write("</tr>\n")
        file.write("</tbody></table>")
        file.write("</div></div>")
        file.write("<div class='row'><div class='col-md-1'>")
        file.write(prev_html)
        file.write("</div><div class='col-md-10'><ul class='pagination' style='float: none;margin: 0 auto;'>")
        for line in prev_pagination:
            file.write(line)
        for line in current_pagination:
            file.write(line)
        for line in next_pagination:
            file.write(line)
        file.write("</ul></div><div class='col-md-1'>")
        file.write(next_html)
        file.write("</div></div>")
        file.write("<hr /><div class='row'><div class='col-md-12'><div class='footer'>")
        file.write(("<p>Marktplaats crawler by <a href='https://raymii.org'>Raymii.org</a>. Parsed marktplaats category <a href='%s'>%s</a>") % (base_url, title))
        file.write("</div></div></div></div></body></html>")
        print("Written overview page to %s" % filename)

def parse_ad_page(page_soup, uid, url):
    """Parses a Marktplaats.nl advertisement page and returns a dict with ad data"""
    content = {}
    content["images"] = []
    for item in page_soup.find_all(attrs={'id': "vip-left-listing"}):
        item_soup = BeautifulSoup(item.encode('utf-8'))
        content["uid"] = uid
        content["url"] = url
        content["title"] = item_soup.find(attrs={'id':'title'}).get_text().encode('utf-8')
        content["title"] = cgi.escape(content["title"])
        content["descr"] = item_soup.find(attrs={'id':'vip-ad-description'}).get_text().encode('utf-8')
        content["views"] = item_soup.find(attrs={'id':'vip-ad-count'}).get_text().encode('utf-8')
        content["price"] = item_soup.find(attrs={'id':'vip-ad-price-container'}).get_text().encode('utf-8')
        content["shipping"] = item_soup.find(attrs={'id':'vip-ad-shipping-cost'}).get_text().encode('utf-8')
        item_photo_carousel = item_soup.find(attrs={'id':'vip-carousel'})
        item_images = item_photo_carousel.attrs['data-images-xl']
        item_image_urls =  item_images.split("&//")
        for image in item_image_urls:
            if image:
                parse = urlparse.urlparse(image)
                content["images"].append("http://" + parse.netloc + parse.path)
    return content

def create_item_page(content, uid):
    """Creates an item advertisement page based on content json and uid. Also downloads and saves all ad images if not exist"""
    create_folder("pages/" + uid)
    if os.path.exists("pages/" + uid + "/index.html") or os.path.exists("pages/" + uid + "/content.json"):
        return True
    print("Creating page for ad %s" % uid)
    with open("pages/" + uid + "/content.json", "w") as file:
        file.write(json.dumps(content))
        file.close()
    with open("pages/" + uid + "/index.html", "wb") as file:
        file.write("<!DOCTYPE html><html lang='en'><head><title>%s</title><link href='http://netdna.bootstrapcdn.com/bootswatch/3.1.1/cosmo/bootstrap.min.css' rel='stylesheet'><meta http-equiv='Content-Type' content='text/html; charset=UTF-8' /></head>" % content["descr"].decode('utf-8').encode('ascii', 'xmlcharrefreplace'))
        file.write('<body><a id="top-of-page"></a> <div class="container-fluid "><div class="row"><div class="col-md-12">')
        file.write("<h1><a href='%s'>%s</a></h1>" % (content["url"], content["title"].decode('utf-8').encode('ascii', 'xmlcharrefreplace')))
        file.write("<table class='table table-striped'>")
        file.write("<tr><td>Beschrijving</td><td>%s</td></tr>" % (content["descr"].decode('utf-8').encode('ascii', 'xmlcharrefreplace')))
        file.write("<tr><td>Prijs</td><td>%s</td></tr>" % (content["price"].decode('utf-8').encode('ascii', 'xmlcharrefreplace')))
        file.write("<tr><td>Views</td><td>%s</td></tr>" % (content["views"].decode('utf-8').encode('ascii', 'xmlcharrefreplace')))
        file.write("<tr><td>Verzendmethode</td><td>%s</td></tr>" % (content["shipping"].decode('utf-8').encode('ascii', 'xmlcharrefreplace')))
        file.write("<tr><td colspan='2'><a href='%s'>View on Marktplaats</a></td></tr>" % content["url"])
        for counter, img_url in enumerate(content["images"]):
            if not os.path.exists("pages/" + uid + "/" + str(counter) + ".jpg"):
                save_image(img_url, "pages/" + uid + "/" + str(counter) + ".jpg") 
            file.write("<tr><td colspan='2'><img src='%s' alt='image' /></td></tr>" % (str(counter) + ".jpg"))
        file.write("</table>")
        file.write("</div></div><hr /><div class='row'><div class='col-md-12'><div class='footer'>")
        file.write("<p>Marktplaats crawler by <a href='https://raymii.org'>Raymii.org</a>.")
        file.write("</div></div></div></div></body></html>")

def get_url_from_uid_json_file(uid):
    """Parse the overview json file and get the item URL from it"""
    with open("pages/" + uid + "/overview_page.json", "r") as file:
        return json.loads(file.read())["item_url"] 

def process_ad_page_full(uid):
    """Does all the functions to get and parse an ad, used for the thread pool"""
    if not os.path.exists("pages/" + uid + "/index.html"):
        url = get_url_from_uid_json_file(uid)
        ad_page_soup = page_to_soup(url)
        content = parse_ad_page(ad_page_soup, uid, url)
        create_item_page(content, uid)

def main():
    global max_pages
    global pax_page_items
    global pool
    global num

    ads_list = []
    for number in range(1,max_pages):
        overview_page_soup = page_to_soup(base_url, number)
        ads_list.append(parse_overview_page(overview_page_soup))
        print("Parsed overview page %i" % number)
    create_ad_overview_json_file(ads_list)
    
    uids = []
    for ads in ads_list:
        for ad in ads:
            uids.append(ad["uid"])
    
    if os.path.exists("ads.json"):
        print("Reading ads.json")
        with open("ads.json", "r") as file:
            try:
                load_uids = json.load(file)
            except ValueError as error:
                print("ads.json exists but error: %s" % error)
                load_uids = []
    else:
        print("Creating ads.json")
        load_uids = []
    
    for item in load_uids:
        uids.append(item)
    
    uids = uniqify(uids)
    with open("ads.json", "w") as file:
        file.write(json.dumps(uids))

    pool = ThreadPool(10)
    uid_pool = pool.map(process_ad_page_full, uids)
    # for uid in write_uids:
    #     process_ad_page_full(uid)
    
    split_uid_list = [uids[x:x+max_page_items] for x in range(0, len(uids),max_page_items)]
    for counter, uid_list in enumerate(split_uid_list):
        counter = counter + 1
        ads_list = []
        for uid in uid_list:
            with open("pages/" + uid + "/overview_page.json") as file:
                ads_list.append(json.load(file))
        max_pages = len(split_uid_list)
        create_overview_page(ads_list, counter, max_pages, "test")


if __name__ == "__main__":
    main()
