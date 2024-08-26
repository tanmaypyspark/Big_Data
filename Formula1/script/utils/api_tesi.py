import os
os.system('cls')
# import json
import requests
import logging
def get_posts():
    url = 'http://ergast.com/api/f1/2008'

    try:
        response = requests.get(url)
        print(logging.info(type(response)))
        print(str(response.json()))
        # if response.status_code == 200:
        #     posts = response.json()
        #     return posts
        # else:
        #     print('Error:', response.status_code)
        #     return None
    except requests.exceptions.RequestException as e:
        print('Error:', e)
        return None

def main():
    posts = get_posts()

    # if posts:
    #     print('First Post Title:', posts[0]['title'])
    #     print('First Post Body:', posts[0]['body'])
    # else:
    #     print('Failed to fetch posts from API.')

class Test:
    def __init__(self,app = 'Test'):
        self.app = app
        # self.name = name
    def display(self,path,name,*kwargs):
        print(kwargs)
        return f'Hi my name is {name}, my address {path}'

if __name__ == '__main__':
    # main()

    t1 = Test()
    t1.display.option('name', 'Tanmay').option('path', 'TATA')