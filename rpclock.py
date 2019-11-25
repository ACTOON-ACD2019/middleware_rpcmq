import asyncio
import base64
import os
import zipfile
import config
from threading import Lock, Thread
from functools import partial
from time import sleep

from aio_pika import connect, IncomingMessage, Exchange, Message


class TaskPoolRpcServer():
    lock_main_thread = None
    lock_rpc = None
    consumer_buffer = None
    consumer_buffer_extension = None
    sender_buffer = None

    def __init__(self):
        self.lock_main_thread = Lock()
        self.lock_rpc = Lock()

    def compress(self, dst_folder):
        zip = zipfile.ZipFile('./result.zip', 'w', zipfile.ZIP_DEFLATED)

        for root, dirs, files in os.walk(dst_folder):
            for file in files:
                zip.write(os.path.join(root, file))

        zip.close()

        return open('./result.zip', 'rb').read()

    def clean_results(self, dir):
      for root, dirs, files in os.walk(dir):
          for file in files:
              os.remove(os.path.join(root, file))

    async def on_message(self, exchange: Exchange, message: IncomingMessage):
        with message.process():
            print('rpc: new message on queue')

            if self.lock_rpc.locked() is not True:
                # after first-pass, will be locked
                self.lock_rpc.acquire()

            print('rpc: passing lock and locking it')

            # decoding arrived image in base64 encoded string
            self.consumer_buffer = base64.b64decode(message.body)
            self.consumer_buffer_extension = message.content_type

            print('rpc: releasing main thread')
            # releasing main thread to work
            self.lock_main_thread.release()

            print('rpc: lock rpc thread')
            # this lock should be released on main thread after dl task finished
            self.lock_rpc.acquire()

            # start working and wait for result
            buf = self.compress(self.sender_buffer)

            # emptying context
            self.consumer_buffer = None
            self.consumer_buffer_extension = None

            await exchange.publish(
                Message(  # self.sender_buffer should be filled
                    body=base64.b64encode(buf),
                    correlation_id=message.correlation_id
                ),
                routing_key=message.reply_to
            )

            self.clean_results(config.TEST_PREDICTION_PATH)

            print('rpc: Request complete')

    async def init(self, loop):
        # Perform connection
        connection = await connect(
            "amqp://username:password@mydomain.me/", loop=loop
        )

        # Creating a channel
        channel = await connection.channel()

        # Declaring queue
        queue = await channel.declare_queue('rpc_queue')

        await queue.consume(
            partial(
                self.on_message,
                channel.default_exchange
            )
        )

    def run(self):
        loop = asyncio.get_event_loop()
        loop.create_task(self.init(loop))

        # we enter a never-ending loop that waits for data
        # and runs callbacks whenever necessary.
        print(" [x] Awaiting RPC requests")
        loop.run_forever()
