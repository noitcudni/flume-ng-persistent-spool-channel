flume-ng-persistent-spool-channel
==================

During a TAKE transaction, instead of writing the data content to disk, simply record the offset and the current filename for replay purposes.

Configuration of Persistent Spool Channel
---------

    agent.channels.persistentSpoolChannel.type = org.apache.flume.channel.PersistentSpoolChannel
    agent.channels.persistentSpoolChannel.capacity = 40000               # shamelessly stolen from MemoryChannel
    agent.channels.persistentSpoolChannel.transactionCapacity = 40000    # shamelessly stolen from MemoryChannel
    agent.channels.persistentSpoolChannel.checkpointDir = [whereever you want]  # default to .flume/persistent-spool-channel/checkpoint

Install as a flume plugin
----------
* Compile this repo
* Copy the uberjar to [whereever flume is]/lib. Example, /usr/lib/flume/lib.
