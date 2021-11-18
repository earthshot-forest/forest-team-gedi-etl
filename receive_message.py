#!/usr/bin/env python
import pika
import sys
import os
import json
from wrap_and_send import *
from gedi_ETL import *    # SR - not working on my computer so commented out
from gedi_utils import * # SR - not working on my computer so commented out

# SR - test data, made it shorter so I could see the results
gedi02_A_dict = {'GEDI02_A': ['https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.09.02/GEDI02_A_2020246094555_O09767_T08357_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.09.01/GEDI02_A_2020245151115_O09755_T08804_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.08.29/GEDI02_A_2020242111938_O09706_T09627_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.08.28/GEDI02_A_2020241164457_O09694_T05958_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.08.25/GEDI02_A_2020238125319_O09645_T03935_02_001_01.h5']}#, 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.08.24/GEDI02_A_2020237181838_O09633_T00266_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.08.20/GEDI02_A_2020233195217_O09572_T04535_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.08.16/GEDI02_A_2020229212552_O09511_T02959_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.08.13/GEDI02_A_2020226173409_O09462_T03935_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.08.12/GEDI02_A_2020225225927_O09450_T04535_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.08.09/GEDI02_A_2020222190743_O09401_T01089_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.08.09/GEDI02_A_2020222003300_O09389_T01689_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.08.05/GEDI02_A_2020218020632_O09328_T02959_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.08.01/GEDI02_A_2020214034001_O09267_T00113_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.07.28/GEDI02_A_2020210051357_O09206_T01536_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.05.16/GEDI02_A_2020137045728_O08074_T01242_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.05.15/GEDI02_A_2020136102255_O08062_T03112_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.05.12/GEDI02_A_2020133063148_O08013_T02665_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.05.07/GEDI02_A_2020128133133_O07940_T01689_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.04.10/GEDI02_A_2020101002520_O07513_T04382_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.04.06/GEDI02_A_2020097203212_O07464_T05511_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.02.23/GEDI02_A_2020054133759_O06793_T01395_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.02.19/GEDI02_A_2020050151139_O06732_T05664_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.02.15/GEDI02_A_2020046164515_O06671_T02818_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.02.07/GEDI02_A_2020038195221_O06549_T04088_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.01.30/GEDI02_A_2020030042435_O06415_T04535_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.01.27/GEDI02_A_2020027003243_O06366_T02665_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.01.23/GEDI02_A_2020023020619_O06305_T05511_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.01.22/GEDI02_A_2020022073155_O06293_T01842_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2020.01.18/GEDI02_A_2020018090701_O06232_T04382_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2019.12.11/GEDI02_A_2019345001829_O05637_T00113_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2019.11.15/GEDI02_A_2019319052304_O05237_T03935_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2019.11.14/GEDI02_A_2019318104857_O05225_T02959_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2019.10.23/GEDI02_A_2019296141853_O04886_T01395_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2019.09.26/GEDI02_A_2019269061820_O04462_T01536_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2019.08.16/GEDI02_A_2019228171258_O03833_T01242_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2019.08.15/GEDI02_A_2019227223906_O03821_T03112_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2019.06.11/GEDI02_A_2019162192037_O02810_T02818_02_001_01.h5', 'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.001/2019.06.11/GEDI02_A_2019162004722_O02798_T00266_02_001_01.h5']}

rab_config = {'username': 'andreotte',
              'password': 'Nasa1\][/.,',
              'host': 'localhost',
              }

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='aoi', durable=True)
    channel.queue_declare(queue='download_parse_upload', durable=True)

    def callback_dpu(ch, method, properties, body):
        """ Callback function for download_parse_upload queue.
        Loads message and calls gedi_dataset_ETL.
        """
        print(" [x] Received %r" % json.loads(body)) # verification of received message
        message = json.loads(body)
        gedi_dataset_ETL(message['url'], message['product'], message['bbox'],
                         message['declared_crs'], message['dataset_label'])  # SR - uncomment this for actual use case
        print(" [x] Done")
        ch.basic_ack(delivery_tag = method.delivery_tag)

    def callback_aoi(ch, method, properties, body):
        """ Callback function for aoi queue
        """
        message = json.loads(body)
        print(" [x] Received %r" % message) # verification of received message

        for i in range(len(message['products'])):
            message['products'][i] = 'GEDI0' + message['products'][i]

        array_of_url_dicts = []
        # SR - for actual use, uncomment below two lines and comment (or delete) the 3rd line below this.
        links = []
        for product in message['products']:
            download_urls = get_gedi_download_links(product, message['version'], message['bbox'])
            for url in download_urls:
                url_message = {'url': url,
                           'product': product,
                           'bbox': message['bbox'],
                           'declared_crs': 4326,
                           'dataset_label': message['dataset_label']}
                [channel, connection] = connect_to_rabbitmq(rab_config)
                produce_rabbitmq_message(channel, connection, 'download_parse_upload', url_message)
        # array_of_url_dicts.append(gedi02_A_dict) # SR - using sample data since above function call not working on my computer
        print(" [x] Done creating url tasks")
        ch.basic_ack(delivery_tag = method.delivery_tag)

    channel.basic_qos(prefetch_count=1) # fairly assigns tasks to many workers
    channel.basic_consume(queue='aoi', on_message_callback=callback_aoi)
    channel.basic_consume(queue='download_parse_upload', on_message_callback=callback_dpu)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming() # enters infinite loop that waits for data and runs
                              # callbacks whenever necessary

# catches KeyboardInterrupt during program shutdown.
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
