import consumer_functions as cf # custom functions for handling these streams

def main():
    """Connect to Twitter stream"""
    cf.connect_to_stream('twitter')

if __name__ == '__main__':
    main()
