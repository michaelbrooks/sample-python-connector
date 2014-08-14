import string
import redis
import time
import os
import re
import threading
import configparser
import src.processor

from src.stream.GnipJsonStreamClient import GnipJsonStreamClient
from src.processor.RedisProcessor import RedisProcessor
from src.utils.Envirionment import Envirionment

######################
# Geters and setters #
######################
_stopped = False


def get_stopped():
    return _stopped


def set_stopped(stopped):
    global _stopped
    _stopped = stopped

##########################
# End geters and setters #
##########################

###################
#### Helpers #####
##################


def repl():
    while not get_stopped():
        cmd = raw_input('> ')
        handle_command(cmd)
        cmd


def handle_command(cmd):
    if not is_empty_string(cmd):
        strip_cmd = re.sub(r'\W', '', cmd)
        try:
            func = commands()[strip_cmd]
        except KeyError:
            handle_unrecognized_command(cmd)
            return
        if func:
            func()


def commands():
    return {
        "redis": redis_processor,
        "stdout": print_stream_processor,
        "mongo": mongo_processor,
        "configure": configure,
        "exit": repl_exit,
        "help": print_help
    }

def handle_unrecognized_command(cmd):
    msg = string.Template("Unrecognized command \"$cmd\"").substitute(cmd=cmd)
    print(msg)

def print_help():
    print help_msg()


def help_msg():
    return """
    Welcome to the Python Thin Connector!
    This is a sample application that demonstrates best practices when
    consuming the Gnip set of streaming APIs

    Commands:

    configure # Run the interactive configuration
    redis # Run the redis
    stdout # Run the stdout processor
  """


def repl_exit():
    print("See you next time! :)")
    set_stopped(True)


def is_empty_string(string):
    pattern = r'\S+.?$'
    result = re.match(pattern, string)
    return result == None


def get_non_null_input(value):
    empty_prompt = string.Template('$value cannot be empty!').substitute(value=value)
    ret_value = None
    while True:
        input = get_user_input_with_prompt(value)
        if not is_empty_string(input):
            ret_value = input
            break
        print empty_prompt
    return ret_value


def get_user_input_with_prompt(value):
    prompt = string.Template('Enter $value: ').substitute(value=value)
    return raw_input(prompt)


def get_user_input_with_default(value, default):
    raw = get_user_input_with_prompt(value)
    ret_val = default if is_empty_string(raw) else raw
    return ret_val


def config_sections():
    return ['redis', 'auth', 'gnacs', 'redis', 'mongo', 'stream', 'sys']


def add_config_sections(config):
    for section in config_sections():
        try:
            config.add_section(section)
        except configparser.DuplicateSectionError:
            # Nothing to see here, as you were
            pass


def setup_config_parser():
    config = configparser.ConfigParser()
    add_config_sections(config)
    return config


def configure():
    config = setup_config_parser()
    config.read(config_file_path())
    gnip_username = get_non_null_input('Gnip username')
    gnip_password = get_non_null_input('Gnip password')
    stream_url = get_non_null_input('Gnip url')
    gnip_streamname = get_non_null_input("Gnip stream name")
    gnip_log_level = get_user_input_with_prompt('log level (fatal, error, info, debug)')
    redis_host = get_user_input_with_default('Redis hostname (default 0.0.0.0)', '0.0.0.0')
    redis_port = get_user_input_with_default('Redis port no (default 6379', '6379')
    mongo_host = get_user_input_with_default('Mongo hostname (default 0.0.0.0)', '0.0.0.0')
    mongo_port = get_user_input_with_default('Mongo port no (default 27017)', '27017')


    config.set('auth', 'username', gnip_username)
    config.set('auth', 'password', gnip_password)
    config.set('stream', 'streamurl', stream_url)
    config.set('stream', 'streamname', gnip_streamname)
    config.set('stream', 'compressed', "True")
    config.set('sys', 'log_level', gnip_log_level)
    config.set('redis', 'host', redis_host)
    config.set('redis', 'port', redis_port)



    write_out_config(config)
    msg = string.Template("Configuration: $config").substitute(config=str(config._sections))
    print(msg)


def config_file_path():
    config_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'config/gnip.cfg')
    return config_file_path


def write_out_config(config):
    with open(config_file_path(), 'r+') as config_file:
        config_file.truncate()
        config.write(config_file)


def mongo_processor():
    stream = setup_client()
    mongo_processor = src.processor.Mongo(stream)
    run_processor(mongo_processor)
    print(
        """Mongo processor finished! Go check the
    Mongo server 'tweets' collection to see what we brought in!"""
    )


def print_stream_processor():
    stream = setup_client()
    run_processor(stream)
    print "\n\n\nWhew! That was a lot of JSON!"


def environment():
    return src.utils.Envirionment()


def start_redis():
    exec "redis-server &" # HAAAAA


def start_mongo():
    exec "mongod &" # Double HAAAAA


def redis_processor():
    start_redis()
    stream = setup_client()
    environment = environment()
    redis_processor = RedisProcessor(stream.queue(), environment)
    flush_redis(environment.redis_host, environment.redis_port)
    run_processor(stream, redis_processor)

    print("""
    Redis processor stopped. Head over to the redis-cli to see what we got!


    hint, run:
    > redis-cli
    > KEYS *    # Command to show all keys
    > llen
    """
    )


def run_processor(client, processor):
    processing_thread = threading.Thread(target=_run_processor, args=(client, processor))
    processing_thread.start()
    print(
        "#{type(processor).__name__} processor started. Press ENTER to stop\n\n"
    )

    while True:
        if raw_input() == "\n":
            break

    print 'Stopping'
    stop_thread = threading.Thread(target=_stop_processor, args=(client, processor))

    while stop_thread.isAlive():
        print '.'
        time.sleep(1)


def _run_processor(client, processor):
    client.run()
    processor.run()


def _stop_processor(client, processor):
    client.stop()
    processor.stop()


def setup_client():
    config = environment()
    return GnipJsonStreamClient(
        config.streamurl,
        config.streamname,
        config.username,
        config.password,
        config.filepath,
        config.rollduration,
        compressed=config.compressed
    )


def flush_redis(host, port):
    client = redis.StrictRedis(host=host, port=port)
    client.flushall()

##################
#  End Helpers
##################

print help_msg()
repl()