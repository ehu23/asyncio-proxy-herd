import sys
import asyncio
import time
import re

    
# GLOBALS

id = ''

# asyncio is not parallelism. It uses one thread to do concurrency (run multiple tasks in an overlapping manner, not necessary parallel) Its similar to using multiple threads and context switching on one CPU

# 'async def' introduces a native coro or an async gen
# 'await' passes function control back to the event loop, suspending the execution of the coroutine that the await expression is in scope of. Ex; if python encounters 'await f()' in the scope of the coro, g(), the await expr tells the event loop to suspend the execution of g() until the thing that its 'a-waiting' on, the result of f(), is returned. In the meantime, go let something else run.

# can only call 'await' on awaitable objects (for now, just await coro's)
# Event loop == while True loop that monitors coroutines. Must put coros on the event loop, otherwise coros are useless
# asyncio.run() gets the event loop, runs tasks until they are marked as complate, and then closes the event loop

# Checks lat/long and time fields. Appends time diff to msg_list
def validate_iamat(msg_list, t):
    # TODO: Check this regex
    iso6709 = re.compile("^[-+]([1-8]?\d(\.\d+)|90(\.0+))[-+](180(\.0+)|((1[0-7]\d)|(0?[1-9]?\d))(\.\d+))$")
    if bool(iso6709.search(msg_list[2])):
        try:
            diff = t - float(msg_list[3])
        except: # ValueError for float()
            return False
        msg_list.append('+{}'.format(diff) if diff >= 0.0 else str(diff))
        return True
    return False

    
async def echo_server(reader, writer):
    print('client connected!')
    while True: # TODO: determine if you need to close the client connection after a query, or leave it open 
        data = await reader.read(100) # 100 bytes
        time_received = time.time()
        msg = data.decode().strip() # decodes into a string

        if msg == 'exit':   # TODO: remove this 
            print('closing clients connection')
            break

        msg_list = msg.split() 
        if len(msg_list) != 4:
            response = 'Queries must have four fields\n'
        else: # Query has 4 fields
            cmd = msg_list[0]
            if cmd == 'IAMAT': 
                if not(validate_iamat(msg_list, time_received)):
                    response = 'Invalid IAMAT fields\n'
                else:
                    # TODO: Make sure responses have newlines or not
                    response = 'AT {} {} {} {} {}\n'.format(id, msg_list[4], msg_list[1], msg_list[2], msg_list[3]) #[4] is the appended diff_time string
            elif cmd == 'WHATSAT':
                response = 'You sent over a WHATSAT query\n'
            else:
                response = 'Invalid command field\n'


        #TODO: Servers should respond to invalid commands with a line that contains a question mark (?), a space, and then a copy of the invalid command.
        writer.write(response.encode())
        await writer.drain() # flow control. pauses the coro from writing more data to the socket until till the client had caught up.
    writer.close() # CLose the connection
    
    

async def main():
    valid_servers = {'Goloman': 11877, 'Hands': 11878, 'Holiday': 11879, 'Welsh': 11880, 'Wilkes': 11881}

    # Check valid args
    if len(sys.argv) != 2:
        print("Incorrect arguments. Must have 1 argument...name of the server\n")
        return

    global id
    id = sys.argv[1]
    if id not in valid_servers:
        print("Invalid server ID.\n Valid: 'Goloman', 'Hands', 'Holiday', 'Welsh', 'Wilkes'\n")
        return


    server = await asyncio.start_server(echo_server, 'localhost', valid_servers[id]) # Creates the server
    print("{} serving on: {}".format(id, server.sockets[0].getsockname())) #TODO: remove this
    await server.serve_forever() # starts serving...forever

if __name__ == "__main__":
    asyncio.run(main())



