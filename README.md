#Gnip Python Sample Connector
This is a sample app which connects to the Gnip set of streaming APIs in Python. The application is broken down into three basic elements:
 - A ```GnipRawStreamClient``` which connects to the HTTP endpoint and buffers the streaming JSON
 - A ```GnipJSONStreamClient``` which wraps the ```GnipRawStreamClient``` and parses the JSON payloads, placing them onto a ```multiprocessing.Queue()```
 - A set of processors which accept a ```multiprocessing.Queue()``` and take action accordingly

Some key notes about this design are as follows:
 1. Modularity: We try to do our best to keep clean separation between different logical pieces of the app.
 2. Multi-Processing: We have separate threads of execution (we are using processes as Python has a global interpreter lock)to handle the different steps of the process. For example, we do not write out to a database on the same thread that we are consuming the data. Rather, we use Queues to communicate between the different parts of the application.
 3. Logging: We log as much relevant data as possible. If this were a production application, we would want to be able to trace what happened when without digging into live code.
 4. Reconnection logic: Sometimes the stream will fail on us. We handle this gracefully by backing off the stream exponentially, and attempting to reconnect until we are successful.

##Requirements

 - Linux (Tested: Ubuntu 14.02, Multiprocessing libraries do not work on OSX)
 - Python (Tested: 2.7)
 - Access to PowerTrack or Decahose stream
 - pip (to install dependencies)
 - mysql-server-5.5 (optional)
 - [Vagrant](https://www.vagrantup.com/) (optional, but we have included a Vagrantfile for convenience)

##Running
 To run with Linux:
 ```bash
 git clone git@github.com:twitterdev/sample-python-connector.git sample-python-connector
 cd sample-python-connector
 sh script/bootstrap.sh
 sh ./start.sh
 ```

 To run with Vagrant:
 ```bash
 vagrant init
 vagrant ssh
 cd /vagrant
 sh ./start.sh
 ```

##Whats Next?
Now that you can access the data, where do you go from here? The world is your
oyster, and we can't wait to see what you do next. There are a few things to keep
in mind however. Getting the data in the door is just the first step, proper storage
of the data for purposes of keeping in compliance is important. The easiest
way to achieve this is to make sure to keep record of both the individual Tweet ids
and user ids, to facilitate removing tweets when necessary.

##LICENCE
Refer to the LICENCE file
