import sys
import asyncio
import time
import re

    
# GLOBALS

valid_servers = {'Goloman': 11877, 'Hands': 11878, 'Holiday': 11879, 'Welsh': 11880, 'Wilkes': 11881}
neighborsOf = {
    'Goloman': ['Hands', 'Holiday', 'Wilkes'],
    'Hands': ['Wilkes', 'Goloman'],
    'Holiday': ['Goloman', 'Welsh', 'Wilkes'],
    'Wilkes': ['Hands', 'Goloman', 'Holiday'],
    'Welsh': ['Holiday']
}
server_id = ''
clients = {}  # Key = 'client'. Value = (sequence number, response list str)



# asyncio is not parallelism. It uses one thread to do concurrency (run multiple tasks in an overlapping manner, not necessary parallel) Its similar to using multiple threads and context switching on one CPU

# 'async def' introduces a native coro or an async gen
# 'await' passes function control back to the event loop, suspending the execution of the coroutine that the await expression is in scope of. Ex; if python encounters 'await f()' in the scope of the coro, g(), the await expr tells the event loop to suspend the execution of g() until the thing that its 'a-waiting' on, the result of f(), is returned. In the meantime, go let something else run.

# can only call 'await' on awaitable objects (for now, just await coro's)
# Event loop == while True loop that monitors coroutines. Must put coros on the event loop, otherwise coros are useless
# asyncio.run() gets the event loop, runs tasks until they are marked as complate, and then closes the event loop
# Wrap coroutine in a Task - is a way to start this coroutine "in background". Adds to event loop but doesn't start right away

# Checks lat/long and time fields. Appends time diff to msg_list
async def validate_iamat(msg_list, t):
    # TODO: Check this regex
    iso6709 = re.compile("^[-+]([1-8]?\d(\.\d+)|90(\.0+))[-+](180(\.0+)|((1[0-7]\d)|(0?[1-9]?\d))(\.\d+))$")
    if bool(iso6709.search(msg_list[2])):
        try:
            diff = t - float(msg_list[3])
        except ValueError:
            return False
        msg_list.append('+{}'.format(diff) if diff >= 0.0 else str(diff))
        return True
    return False

async def iamat_response_handler(response_str):
    rlist = response_str.split() # split does not care if it ends in a newline (or any number of white spaces)
    # if its not in clients, or its in clients, check if time is greater
    global clients
    cid = rlist[3]
    if cid not in clients:
        clients[cid] = (0, rlist)
        asyncio.create_task(flood(0, response_str))
    elif float(rlist[5]) > float(clients[cid][1][5]):
        sn = clients[cid][0] + 1 
        clients[cid] = (sn, rlist)
        asyncio.create_task(flood(sn, response_str))
    # else, its value in clients is the most recent already. So do nothing.

# resp = response string
async def flood(sn, resp):
    for neighbor in neighborsOf[server_id]:
        try:
            reader, writer = await asyncio.open_connection('localhost', valid_servers[neighbor]) 
            writer.write("FLOOD {} {}\n".format(sn, resp).encode())
            await writer.drain()
            writer.close()
        except:
            print('error flooding')

async def handle_connection(reader, writer):
    print('client connected!')
    global clients
    while True: # TODO: determine if you need to close the client connection after a query, or leave it open 
        data = await reader.read(100) # 100 bytes
        time_received = time.time()
        error = False
        msg = data.decode().strip() # decodes into a string
        msg_list = msg.split() 

        if len(msg_list) == 4:
            cmd = msg_list[0]

            if cmd == 'IAMAT': 
                if not(await validate_iamat(msg_list, time_received)):
                    response = 'Invalid IAMAT fields\n'
                else:
                    # TODO: Make sure responses have newlines or not
                    # TODO: might want to send all responses right away if we have awaits
                    response = 'AT {} {} {} {} {}\n'.format(server_id, msg_list[4], msg_list[1], msg_list[2], msg_list[3]) #[4] is the appended diff_time string
                    await iamat_response_handler(response)

            elif cmd == 'WHATSAT': 
                try:
                    radius = float(msg_list[2])
                    upper_bound = int(msg_list[3])
                except ValueError:
                    response = 'Invalid WHATSAT fields\n'
                    error = True

                if not error:
                    if msg_list[1] not in clients:
                        response = 'Not a client\n'
                    else:
                        response = 'WHATSAT response w/ client info\n'
            else:
                response = 'Invalid command field 4\n'

        elif len(msg_list) == 8:
            if msg_list[0] == 'FLOOD':
                # TODO: check for syntax of fields, but can assume its fine because this is custom

                # if incoming flood msg is a client we don't have or
                # we do have, but its seq. num is greater than ours
                cid = msg_list[5]
                sn = int(msg_list[1])
                if (cid not in clients) or (sn > clients[cid][0]):
                    clients[cid] = (sn, msg_list[2:])
                    asyncio.create_task(flood(sn, ''.join(msg_list[2:])))
                elif float(msg_list[7]) > float(clients[cid][1][5]): # (sn <= clients[cid][0]) and this msg's time is greater than the one we have stored. Meaning: this came from a server that crashed, and the client is more recent
                    new_sn = clients[cid][0]+1 # increment the sn on this server
                    clients[cid] = (new_sn, msg_list[2:])
                    asyncio.create_task(flood(new_sn, ''.join(msg_list[2:])))
                # else, we ignore this flood message 
                response = 'Flooding\n'

            else:
                response = 'Invalid command field 8\n'
        else:
            response = 'Invalid number of fields\n'

        #TODO: Servers should respond to invalid commands with a line that contains a question mark (?), a space, and then a copy of the invalid command.
        writer.write(response.encode())
        await writer.drain() # flow control. pauses the coro from writing more data to the socket until till the client had caught up.
    writer.close() # CLose the connection
    
    

async def main():

    # Check valid args
    if len(sys.argv) != 2:
        print("Incorrect arguments. Must have 1 argument...name of the server\n")
        return

    global server_id
    server_id = sys.argv[1]
    if server_id not in valid_servers:
        print("Invalid server ID.\n Valid: 'Goloman', 'Hands', 'Holiday', 'Welsh', 'Wilkes'\n")
        return

    server = await asyncio.start_server(handle_connection, 'localhost', valid_servers[server_id]) # Creates the server
    print("{} serving on: {}".format(server_id, server.sockets[0].getsockname())) #TODO: remove this
    await server.serve_forever() # starts serving...forever
# The first arg to start_server is a callback, called whenever a new client connection is established. Since its a coro, it will automatically be scheduled as a task. 

if __name__ == "__main__":
    asyncio.run(main())



