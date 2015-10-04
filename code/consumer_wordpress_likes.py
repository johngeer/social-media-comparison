import consumer_functions as cf # custom functions for handling these streams

def main():
    """Connect to WordPress.com stream"""
    cf.connect_to_stream('likes')

if __name__ == '__main__':
    main()
