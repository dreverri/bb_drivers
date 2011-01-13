package com.basho.riak.jinterface;
import java.util.logging.*;
import com.ericsson.otp.erlang.*;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.MapReduceResponse;

/*
  Interface to Riak Java Client for Basho Bench.
*/
public class App
{
    private static Logger logger = Logger.getLogger("com.basho.riak.jinterface");

    private final OtpErlangAtom ok = new OtpErlangAtom("ok");
    private final OtpErlangAtom notfound = new OtpErlangAtom("notfound");
    private final OtpErlangAtom error = new OtpErlangAtom("error");
    private final OtpErlangAtom unknown = new OtpErlangAtom("unknown");

    private RiakClient riak;
    private OtpNode node;
    private OtpMbox mbox;
    private FileHandler handler;


    public static void main( String[] args )
    {
        /*
          Commands:
          create(Bucket, Key, Value) -> ok|error
          read(Bucket, Key) -> ok|notfound|error
          update(Bucket, Key, Value) -> ok|notfound|error
          delete(Bucket, Key) -> ok
          mapred(Query) -> ok|error

          Command format:
          {From :: OtpErlangPid, Cmd :: OtpErlangAtom, Args :: OtpErlangList}
        */
        String name = new String(args[0]);
        String url = new String(args[1]);
        new App(name, url).loop();
    }

    public App(String name, String url)
    {
        try {
            handler = new FileHandler("riak-java-client.log", true);
            handler.setLevel(Level.ALL);
            logger.addHandler(handler);
            logger.setLevel(Level.ALL);

            node = new OtpNode(name);
            mbox = node.createMbox("mbox");
            riak = new RiakClient(url);
        } catch (Exception e) {
            logger.warning("App() " + e);
        }
    }

    private void loop()
    {
        boolean running = true;
        while(running) {
            try {
                OtpErlangObject o = mbox.receive();
                OtpErlangTuple msg = (OtpErlangTuple) o;
                OtpErlangPid from = (OtpErlangPid) msg.elementAt(0);
                OtpErlangAtom cmd = (OtpErlangAtom) msg.elementAt(1);
                OtpErlangList args = (OtpErlangList) msg.elementAt(2);

                try {
                    // route cmd
                    String c = cmd.atomValue();
                    if (c.equals("create")) {
                        create(from, args);
                    } else if (c.equals("read")) {
                        read(from, args);
                    } else if (c.equals("update")) {
                        update(from, args);
                    } else if (c.equals("delete")) {
                        delete(from, args);
                    } else if (c.equals("mapred")) {
                        mapred(from, args);
                    } else {
                        logger.warning("unknown command " + c);
                        mbox.send(from, unknown);
                    }
                } catch (Exception e) {
                    mbox.send(from, error);
                    throw e;
                }
            } catch (Exception e) {
                logger.warning("loop() " + e);
            }
            handler.flush();
        }
    }

    // args :: [Bucket, Key, Value]
    // reply :: ok|error
    private void create(OtpErlangPid from, OtpErlangList args)
    {
        String bucketStr = binary_to_string(args, 0);
        String keyStr = binary_to_string(args, 1);
        String valueStr = binary_to_string(args, 2);

        RiakObject o = new RiakObject(bucketStr, keyStr, valueStr);

        StoreResponse r = riak.store(o);

        if (r.isSuccess()) {
            mbox.send(from, ok);
        } else {
            mbox.send(from, error);
        }
    }

    // args :: [Bucket, Key]
    // reply :: ok|notfound|error
    private void read(OtpErlangPid from, OtpErlangList args)
    {
        String bucketStr = binary_to_string(args, 0);
        String keyStr = binary_to_string(args, 1);

        FetchResponse r = riak.fetch(bucketStr, keyStr);

        if (r.isSuccess()) {
            mbox.send(from, ok);
        } else {
            mbox.send(from, notfound);
        }

    }

    // args :: [Bucket, Key, Value]
    // reply :: ok|notfound|error
    private void update(OtpErlangPid from, OtpErlangList args)
    {
        String bucketStr = binary_to_string(args, 0);
        String keyStr = binary_to_string(args, 1);
        String valueStr = binary_to_string(args, 2);

        FetchResponse fr = riak.fetch(bucketStr, keyStr);

        if (fr.isSuccess()) {
            RiakObject o = fr.getObject();
            o.setValue(valueStr);
            StoreResponse sr = riak.store(o);

            if (sr.isSuccess()) {
                mbox.send(from, ok);
            } else {
                mbox.send(from, error);
            }
        } else {
            mbox.send(from, notfound);
        }
    }

    // args :: [Bucket, Key]
    // reply :: ok|error
    private void delete(OtpErlangPid from, OtpErlangList args)
    {
        String bucketStr = binary_to_string(args, 0);
        String keyStr = binary_to_string(args, 1);

        riak.delete(bucketStr, keyStr);
        mbox.send(from, ok);
    }

    // args :: [QueryString]
    // reply :: ok|error
    private void mapred(OtpErlangPid from, OtpErlangList args)
    {
        String queryStr = binary_to_string(args, 0);

        MapReduceResponse r = riak.mapReduce(queryStr);

        if (r.isSuccess()) {
            mbox.send(from, ok);
        } else {
            mbox.send(from, error);
        }
    }

    private String binary_to_string(OtpErlangList args, int arg)
    {
        OtpErlangBinary bin = (OtpErlangBinary) args.elementAt(arg);
        String str = new String(bin.binaryValue());
        return str;
    }
}
