"""
    Emily Zimmerman -- 2/14/2023 -- This program takes in multiple columns and rows from a csv file and sends them to RabbitMQ 
"""

import pika
import sys
import webbrowser
import csv
import time

#Sets up a direct link to rabbitmq server.
def offer_rabbitmq_admin_site(show_offer):
    if show_offer == True:
        """Offer to open the RabbitMQ Admin website"""
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

def delete_queue(host:str, queue_name: str):
    # create a blocking connection to the RabbitMQ server
    conn = pika.BlockingConnection(pika.ConnectionParameters(host))
    # use the connection to create a communication channel
    ch = conn.channel()
    #Delete the queue!
    ch.queue_delete(queue_name)


def send_message(host: str, queue_name: str, message):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.
    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()


def csv_processor(input_file):

    with open(input_file, 'r') as input_file:

        reader = csv.reader(input_file, delimiter=",")
        #Skip the header!
        next(reader)
        for column in reader:

            #Grab all of the data from the columns
            timemessage = f"{column[0]}"
            smokermessage = f"{column[1]}"
            foodAtempmessage = f"{column[2]}"
            foodBtempmessage = f"{column[3]}"

            #Creating the fstring tuples
            timesmokermessage = f"{timemessage},{smokermessage}"
            timefoodAmessage = f"{timemessage},{foodAtempmessage}"
            timefoodBmessage = f"{timemessage},{foodBtempmessage}"

            #Encoding all messages
            timesmokermessage = timesmokermessage.encode()
            timefoodAmessage = timefoodAmessage.encode()
            timefoodBmessage = timefoodBmessage.encode()
        
            #Sending each message to the queues!
            send_message("localhost", "01-smoker", timesmokermessage)
            send_message("localhost", "02-food-A", timefoodAmessage)
            send_message("localhost", "03-food-B", timefoodBmessage)

    # sleep for a few seconds
    time.sleep(30)

    #Close the file
    input_file.close



# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site(True)

    #Be sure to delete the queues so we do not overload
    delete_queue("localhost", "01-smoker")
    delete_queue("localhost", "02-food-A")
    delete_queue("localhost", "03-food-B")

    #Run program with our file!
    csv_processor('smoker-temps.csv')
