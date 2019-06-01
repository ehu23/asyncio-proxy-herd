import sys
import asyncio

    
# GLOBALS

id = ''

# asyncio is not parallelism. It uses one thread to do concurrency (run multiple tasks in an overlapping manner, not necessary parallel) Its similar to using multiple threads and context switching on one CPU

# 'async def' introduces a native coro or an async gen
# 'await' passes function control back to the event loop, suspending the execution of the coroutine that the await expression is in scope of. Ex; if python encounters 'await f()' in the scope of the coro, g(), the await expr tells the event loop to suspend the execution of g() until the thing that its 'a-waiting' on, the result of f(), is returned. In the meantime, go let something else run.

# can only call 'await' on awaitable objects (for now, just await coro's)
# Event loop == while True loop that monitors coroutines. Must put coros on the event loop, otherwise coros are useless
# asyncio.run() gets the event loop, runs tasks until they are marked as complate, and then closes the event loop

async def echo_server(reader, writer):
    print('client connected!')
    while True: # TODO: determine if you need to close the client connection after a query, or leave it open 
        data = await reader.read(100) # 100 bytes
        msg = data.decode().strip() # decodes into a string

        if msg == 'exit':   # TODO: remove this 
            print('closing clients connection')
            break

        msg_list = msg.split() 
        if len(msg_list) != 4:
            writer.write('Queries must have four fields\n'.encode())
        else: # Query has 4 fields
            cmd = msg_list[0]
            if cmd == 'IAMAT':
               response = 'AT {} +21.32 {} {} {}\n'.format(id, msg_list[1], msg_list[2], msg_list[3]) 
               writer.write(response.encode())
            elif cmd == 'WHATSAT':
                writer.write('You sent over a WHATSAT query\n'.encode())
            else:
                writer.write('Invalid command field\n'.encode())


        await writer.drain() # flow control. pauses the coro from writing more data to the socket until till the client had caught up.
    writer.close() # CLose the connection
    
    

async def main():
    valid_servers = ['Goloman', 'Hands', 'Holiday', 'Welsh', 'Wilkes']

    # Check valid args
    if len(sys.argv) != 2:
        print("Incorrect arguments. Must have 1 argument...name of the server\n")
        return
    if sys.argv[1] not in valid_servers:
        print("Invalid server ID.\n Valid: 'Goloman', 'Hands', 'Holiday', 'Welsh', 'Wilkes'\n")
        return

    print("Server name: %s" % (sys.argv[1])) #TODO: remove this
    global id
    id = sys.argv[1]

    server = await asyncio.start_server(echo_server, 'localhost', 5000) # Creates the server
    await server.serve_forever() # starts serving...forever

if __name__ == "__main__":
    asyncio.run(main())



