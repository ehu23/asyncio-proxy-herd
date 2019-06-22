import sys
import asyncio
import time
import re
import aiohttp
import json

    
# GLOBALS
valid_servers = {
    'Goloman': 11877, 
    'Hands': 11878, 
    'Holiday': 11879, 
    'Welsh': 11880, 
    'Wilkes': 11881
    }
neighborsOf = {
    'Goloman': ['Hands', 'Holiday', 'Wilkes'],
    'Hands': ['Wilkes', 'Goloman'],
    'Holiday': ['Goloman', 'Welsh', 'Wilkes'],
    'Wilkes': ['Hands', 'Goloman', 'Holiday'],
    'Welsh': ['Holiday']
    }
server_id = ''
clients = {}  # Key = 'client'. Value = (sequence_number, response (tokenized into a list of strings))
log = None
places_api = 'INSERT_API_HERE'



# ----------------------ASYNCIO NOTES-------------------------


# asyncio is not parallelism. It uses one thread to do concurrency (run multiple tasks in an overlapping manner, not necessary parallel) Its similar to using multiple threads and context switching on one CPU

# 'async def' introduces a native coro or an async generator

# 'await' passes function control back to the event loop, suspending the execution of the coroutine that the await expression is in scope of.
# Ex; if python encounters 'await f()' in the scope of the coro, g(), the await expr tells the event loop to suspend the execution of g() until the thing that its 'a-waiting' on, the result of f(), is returned. In the meantime, go let something else run.

# You can only call 'await' on awaitable objects (for this project, just await coro's)

# Event loop == while True loop that monitors coroutines. Must put coros on the event loop, otherwise coros are useless

# Wrap coroutine in a Task - is a way to start this coroutine "in the background". This adds it to the event loop, but doesn't start it right away




# Checks lat/long and time fields (returns True/False). Appends time diff to msg_list if True
async def validate_iamat(msg_list, t):
    iso6709 = re.compile("^[-+]([1-8]?\d(\.\d+)|90(\.0+))[-+](180(\.0+)|((1[0-7]\d)|(0?[1-9]?\d))(\.\d+))$")
    if bool(iso6709.search(msg_list[2])):
        try:
            diff = t - float(msg_list[3])
        except ValueError:
            return False
        msg_list.append('+{}'.format(diff) if diff >= 0.0 else str(diff))
        return True
    return False


# Note: .split() does not care if the passed in str ends in a newline (or any number of white spaces)
# Handles updates if this IAMAT is new/newer
async def iamat_response_handler(response_str):
    rlist = response_str.split() 
    global clients
    cid = rlist[3]

    if cid not in clients:
        clients[cid] = (0, rlist)
        asyncio.create_task(flood(0, response_str))
    elif float(rlist[5]) > float(clients[cid][1][5]): # if its more recent
        sn = clients[cid][0] + 1 
        clients[cid] = (sn, rlist)
        asyncio.create_task(flood(sn, response_str))
    # else, our cache is the most recent. So do nothing.


# Note: resp = response string
async def flood(sn, resp):
    global log
    for neighbor in neighborsOf[server_id]:
        log.write("OPENING a connection with {}...".format(neighbor)) 
        try:
            reader, writer = await asyncio.open_connection('localhost', valid_servers[neighbor]) 
            log.write("SUCCESS\n\tFLOODING: FLOOD {} {}\n".format(sn, resp))
            writer.write("FLOOD {} {}\n".format(sn, resp).encode())
            await writer.drain()
            writer.close()
        except:
            log.write("FAIL\n")


# ll_str format: +/-Float+/-Float. Return: lat,lon w/o '+'
def parseLL(ll_str): 
    count = 0
    for i, c in enumerate(ll_str):
        if c == '+' or c == '-':
            count += 1
            if count == 2:
                break

    # i points to second '+'/'-'
    lat = ll_str[:i]
    lon = ll_str[i:]
    lat = lat.replace('+','')
    lon = lon.replace('+','')

    return lat + ',' + lon
    

async def getJSON(url, num_results):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            json_reply = await resp.json()
            json_reply['results'] = json_reply['results'][:num_results]
            json_str = '' + json.dumps(json_reply, indent=3)
            return json_str



# Note: flow control. pauses the coro from writing more data to the socket until till the client had caught up.
async def handle_connection(reader, writer):
    global clients
    global log

    data = await reader.read() # Reads until EOF
    time_received = time.time()
    error = False
    flood_msg = False

    msg = data.decode().strip()
    msg_list = msg.split() 
    log.write("RECEIVED: " + data.decode())

    if len(msg_list) == 4:
        cmd = msg_list[0]
        cid = msg_list[1]

        if cmd == 'IAMAT': 
            if not(await validate_iamat(msg_list, time_received)):
                error = True
            else:
                # Future Note: might want to send all responses right away if we have awaits
                # Recall: msg_list[4] is the appended diff_time from validate_iamat()
                response = 'AT {} {} {} {} {}\n'.format(server_id, msg_list[4], cid, msg_list[2], msg_list[3]) 
                await iamat_response_handler(response)

        elif cmd == 'WHATSAT': 
            # Validate WHATSAT fields
            try:
                radius = float(msg_list[2])*1000
                num_results = int(msg_list[3])
            except ValueError:
                error = True

            if not error:
                if radius <= 0 or radius > 50000 or num_results <= 0 or num_results > 20:
                    error = True 
                elif cid not in clients:
                    error = True
                else:
                    client_info = clients[cid][1]
                    latlong = parseLL(client_info[4])
                    url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?key={}&radius={}&location={}'.format(places_api, radius, latlong)
                    response = ' '.join(client_info) + '\n' + await getJSON(url, num_results) + '\n\n'
        else:
            error = True

    elif len(msg_list) == 8:
        # Future Note: We can assume fields are fine because this is only sent from servers
        if msg_list[0] == 'FLOOD':
            flood_msg = True
            cid = msg_list[5]
            sn = int(msg_list[1])

            # Update if incoming flood msg is a client we don't have or we do have, but its seq. num is greater than ours
            if (cid not in clients) or (sn > clients[cid][0]):
                clients[cid] = (sn, msg_list[2:])
                asyncio.create_task(flood(sn, ' '.join(msg_list[2:])))
            # Or, (sn <= clients[cid][0]) and if this msg's time is greater than the one we have stored. 
            #     Meaning: this came from a server that crashed, and the client is more recent
            elif float(msg_list[7]) > float(clients[cid][1][5]): 
                new_sn = clients[cid][0]+2 # new_sn is based on this server's sn+2 (+2 just for propagation-race-safety)
                clients[cid] = (new_sn, msg_list[2:])
                asyncio.create_task(flood(new_sn, ' '.join(msg_list[2:])))
            # else, we ignore this flood msg 
        else:
            error = True
    else:
        error = True

    if error:
        response = '? ' + data.decode()
    if not flood_msg:
        writer.write(response.encode())
        await writer.drain() 
        log.write("SENT: {}".format(response))

    # Close the connection
    writer.close() 


# Note: The first arg to start_server is a callback, called whenever a new client connection is established. Since its a coro, it will automatically be scheduled as a task. 
async def main():

    global log
    global server_id

    # Check valid args
    if len(sys.argv) != 2:
        print("Incorrect arguments. Must have 1 argument...name of the server\n")
        return

    server_id = sys.argv[1]
    if server_id not in valid_servers:
        print("Invalid server ID.\n Valid: 'Goloman', 'Hands', 'Holiday', 'Welsh', 'Wilkes'\n")
        return

    # Create a log file
    log = open("{}_log.txt".format(server_id), "w+")

    # Create server
    server = await asyncio.start_server(handle_connection, 'localhost', valid_servers[server_id]) 
    # print("{} serving on: {}".format(server_id, server.sockets[0].getsockname())) 
    
    # Run server
    try:
        await server.serve_forever()
    finally:
        if log is not None:
            log.close()    


# Note: asyncio.run() gets the event loop, runs tasks until they are marked as complete, and then closes the event loop
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass



