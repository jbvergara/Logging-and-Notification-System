import csv, requests, logging, os, sys
from uuid import uuid4

#Declare Environment Variables
api_endpoint = 'https://l2jy5gomd9.execute-api.ap-southeast-1.amazonaws.com/default/jem-testfunc'
source_application = os.path.basename(__file__)
log_filename = 'buff.log'


#Configure Logging
log_format = ('[%(asctime)s],%(levelname)s,%(module)s,%(lineno)d,%(message)s')
logging.basicConfig(
    level=logging.DEBUG,
    format=log_format,
    datefmt='%m-%d-%Y %H:%M:%S',
    filename=log_filename,
    filemode='w'
)
log = logging.getLogger(__name__)

#Logger Function
def logger_func(log_level, message, unique_id, details=None):
    data = {
        'body':{
            "log_level": log_level,
            "message": message,
            "details": details,
            "source_application": source_application,
            "id": str(uuid4())
        }
    }
    response = requests.post(api_endpoint, json=data)
    print(response.json())

def post_logs(filename):
    #Extract all logs
    with open(filename, 'r') as log_file:
        fieldnames = 'time', 'level', 'module', 'line', 'message'
        table = csv.DictReader(
            log_file,
            fieldnames = fieldnames
        )
        #Create a snapshot of the log file
        listoflists = []
        for row in table:
            buff=[]
            for field in fieldnames:
                buff.append(row[field])
            listoflists.append(buff)
    #Post all the logs from the snapshot
    for item in listoflists:
        logger_func(
            item[1].lower(), #log_level
            item[4], #message
            item[0], #timestamp
            details=f'{item[0]}: {item[1]} - {item[2]} - {item[3]}' #details
        )

def csv_filter(filename, filter_col):
    try:
        with open(filename, 'r') as csv_file:
            row1 = csv.reader(csv_file)
            fieldnames = next(row1)
            table = csv.DictReader(
                csv_file,
                fieldnames = fieldnames
            )
            listoflists = []
            for row in table:
                if row[filter_col] != '':
                    buff = []
                    for field in fieldnames:
                        buff.append(row[field])
                    listoflists.append(buff)
            return listoflists, fieldnames
            
    except KeyError:
        log.warning(f'Invalid Fieldname: {filter_col}')
        post_logs(log_filename)
        sys.exit(1)
        
    except UnicodeDecodeError:
        log.error(f'Invalid File: {filename}')
        post_logs(log_filename)
        sys.exit(1)

def csv_rewriter(filename, fieldnames, listoflists):
    with open(filename + '_parsed.csv', 'w') as gen_csv:
        writer = csv.writer(gen_csv)
        writer.writerow(fieldnames)
        for item in listoflists:
            writer.writerow(item)
            
def csv_downloader(url):
    try:
        filename = url.split('/')[-1]
        filename = filename.split('.')[0]
        r = requests.get(url)
        r.raise_for_status()
        with open(filename, 'wb') as file:
            file.write(r.content)
        log.info('File Downloaded Successfully')
        return filename
        
    except requests.HTTPError:
        log.critical(f'Invalid URL: {url}')
        post_logs(log_filename)
        sys.exit(1)

def main():
    url = input("Please enter the URL of the CSV file: ")
    filename = csv_downloader(url)
    listoflists, fieldnames = csv_filter(filename, 'Categories')
    csv_rewriter(filename, fieldnames, listoflists)
    log.info('Done')
    post_logs(log_filename)
    sys.exit(0)
            
if __name__ == '__main__':
    main()


        