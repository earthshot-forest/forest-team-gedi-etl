#!/usr/bin/env python
import pika
import sys
import json
import re

def is_IPv4(address: str) -> bool:
    if type(address) != type(''): return False

    if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", address):
        address_arr = address.split('.')
        validator_arr = []

        for num in address_arr:
            if (int(num) >= 0 and int(num) <= 255):
                validator_arr.append(True)
            else:
                validator_arr.append(False)

        return all(validator_arr)
    else:
        return False

def valid_host(address: str) -> bool:
    # can use this function later to specify specific allowable IP addresses
    return address == 'localhost' or is_IPv4(address)

def valid_user(config: dict) -> bool:
    return True # placeholder

def valid_rab_config(config: dict) -> bool:
    """ A valid config dict has to have:
        - username, string
        - password, string
        - host, number string matching IPv4 address or 'localhost'
    """
    if type(config) != type({}):
        return False
    else:
        for key in ['username', 'password', 'host']:
            if key not in config.keys():
                return False
            elif type(config[key]) != type(''):
                return False

        return valid_host(config['host']) and valid_user(config)

def in_range(num: float, lower_bound: int, upper_bound: int) -> bool:
    return num >= lower_bound and num <= upper_bound

def elements_are_strings(array: list) -> bool:
    for el in array:
        if type(el) != type(''):
            return False

    return True

def valid_bounding_box(bbox: dict) -> bool:
    """ Determines whether the coordinates in the bounding box are within
    the min & max longitude and latitudes. It also determines whether the bbox
    is formatted correctly, meaning the values are strings and the keys are
    valid.

    A possible rewrite of this function could be used to ensure the total area
    of the bbox is within a certain size.
    """
    [MAX_LAT, MIN_LAT, MAX_LON, MIN_LON] = [51.6, -51.6, 180, -180]

    keys = list(bbox.keys())
    keys.sort()
    valid_keys = keys == ['lr_lat', 'lr_lon', 'ul_lat', 'ul_lon']
    values_are_strings = elements_are_strings(list(bbox.values()))

    return (valid_keys and values_are_strings and
            in_range(float(bbox['lr_lat']), MIN_LAT, MAX_LAT) and
            in_range(float(bbox['ul_lat']), MIN_LAT, MAX_LAT) and
            in_range(float(bbox['lr_lon']), -180, 180) and
            in_range(float(bbox['ul_lon']), -180, 180))

def valid_products(products: list) -> bool:
    POSSIBLE_PRODUCTS = ['1_A', '1_B', '2_A', '2_B', '4_A', '4_B']
    if type(products) == type([]):
        for product in products:
            if product not in POSSIBLE_PRODUCTS:
                return False

        return True
    else:
        return False

def valid_version(version: str) -> bool:
    POSSIBLE_VERSIONS = ['001'] # full list of valid versions for now
    return version in POSSIBLE_VERSIONS

def connect_to_rabbitmq(rab_config: dict) -> list:
    """Connects to RabbitMQ server.
    :param rab_config, a dictionary containing config data with keys:
        'host': 'localhost' or IPv4 address of server
        'username': username
        'password': password

    """
    if valid_rab_config(rab_config):
        connection = pika.BlockingConnection(pika.ConnectionParameters(rab_config['host']))
        channel = connection.channel()

        channel.queue_declare(queue='aoi', durable=True)
        channel.queue_declare(queue='download_parse_upload', durable=True)
        return [channel, connection]
    else:
        return None

def produce_gedi_download_tasks(bbox: dict, products: list, version: str, dataset_label: str) -> dict:
    """Produce a dict message that can be passed into produce_rabbitmq_message
    :param bbox: An area of interest as a dictionary containing keys
        'ul_lat', 'ul_long', 'lr_lat', 'lr_lon'
    :param products: The GEDI product.
        Options - 1_A, 1_B, 2_A, 2_B, 4_A, 4_B
    :param version: The GEDI production version. Option - '001'
    """
    valid_inputs = (valid_bounding_box(bbox) and valid_products(products)
                                             and valid_version(version))
    if valid_inputs:
        return {'products': products,
                'version': version,
                'bbox': bbox,
                'dataset_label': dataset_label}
    else:
        return None

def produce_rabbitmq_message(channel, connection, routing_key: str, message: dict):
    """ Sends a rabbitmq message
    :param channel: A rabbitmq channel created by connect_to_rabbitmq()
    :param connection: A rabbitmq connection created by connect_to_rabbitmq()
    :param routing_key: Name of queue, valid names are 'aoi' and 'download_parse_upload'
    :param message: The data being sent.
    """
    valid_channel = channel != None
    valid_routing_key = routing_key in ['aoi', 'download_parse_upload']
    valid_message = message != None

    if valid_channel and valid_routing_key and valid_message:
        channel.basic_publish(exchange='',
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(
            delivery_mode=2, # makes message persistent
        ))

        print(" [x] Sent %r" % message) # confirmation of message sent
        connection.close() # close connection
    else:
        print('invalid routing key')


def main():
    rab_config = {'username': '',
                  'password': '',
                  'host': 'localhost',
                  }

    bbox = {'ul_lat': '44.0285565760225026',
             'ul_lon': '-122.2452749872689992',
             'lr_lat': '44.1375557115117019',
             'lr_lon': '-122.4007032176819934'}

    products = ['1_B', '2_A', '2_B']

    version = '001'

    message = {'products': products,
               'version': version,
               'bbox': bbox}
    dataset_label = 'test_data'
    # actual use, aoi
    [channel, connection] = connect_to_rabbitmq(rab_config)
    message = produce_gedi_download_tasks(bbox, products, version, dataset_label)
    produce_rabbitmq_message(channel, connection, 'aoi', message)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
