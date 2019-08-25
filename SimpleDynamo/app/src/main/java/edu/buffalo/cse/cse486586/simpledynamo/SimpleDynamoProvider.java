package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    String successorPort = "11112";
    String predecessorPort = "11124";
    static String myPortID;
    static String myPort;
    static boolean lock = false;
    final int SERVER_PORT = 10000;
    static boolean isDone = false;
    static boolean hasRestarted = false;
    static String entryPort;

    static String[] PRED2 = {"5558", "5560", "5562", "5556", "5554"};
    static String[] PRED1 = {"5560", "5562", "5556", "5554", "5558"};
    static String[] ORDER = {"5562", "5556", "5554", "5558", "5560"};
    static String[] SUCC1 = {"5556", "5554", "5558", "5560", "5562"};
    static String[] SUCC2 = {"5554", "5558", "5560", "5562", "5556"};


    static ArrayList<String> ownData = new ArrayList<String>();
    static ArrayList<String> pred1Data = new ArrayList<String>();
    static ArrayList<String> pred2Data = new ArrayList<String>();

    Map<String, String> keyVersionTracker = new HashMap<String, String>();
    MatrixCursor globalDataCursor = new MatrixCursor(new String[]{"key", "value"});


    public boolean onCreate() {
        // TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = Integer.toString(Integer.parseInt(portStr) * 2);
        Log.d("INIT", "My port is " + myPort);
        successorPort = myPort;
        predecessorPort = myPort;
        entryPort = myPort;
//        if (fileExist("hasRestarted")) {
//            hasRestarted = Boolean.parseBoolean((getFiles("hasRestarted")[1]).toString());
//            Log.d("HAS_RESTARTED,",Boolean.toString(hasRestarted));
//        }
        try {
            myPortID = genHash(portStr);
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
//            if (hasRestarted) {
//
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            new ArchiveTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portStr);

            //            }
//            FileOutputStream outputStream;
//            outputStream = getContext().openFileOutput("hasRestarted", Context.MODE_PRIVATE);
//            outputStream.write(Boolean.toString(!hasRestarted).getBytes());
//            Log.d("HAS_RESTARTED_CHECK",Boolean.toString(!hasRestarted));


        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IOException e) {
            Log.d("SOCKETERROR", "IOException raised");
            e.printStackTrace();
        }
        return false;
    }

    public int getMyIndex(String port) {
        for (int index = 0; index < ORDER.length; index++) {
            if (port.equals(ORDER[index])) {
                return index;
            }
        }
        return 0;
    }

    public boolean isStorable(String key, int index) {
        try {
            if ((genHash(key).compareTo(genHash(ORDER[index])) <= 0) || (genHash(key).compareTo(genHash(PRED1[index])) <= 0) || (genHash(key).compareTo(genHash(PRED2[index]))) <= 0) {
                return true;
            } else if (genHash(key).compareTo(genHash(ORDER[ORDER.length - 1])) > 0 && ORDER[index].equals(ORDER[0])) {
                return true;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean fileExist(String fname) {
        File file = getContext().getFileStreamPath(fname);
        return file.exists();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        keyVersionTracker.clear();
        File[] files = getContext().getFilesDir().listFiles();
        Log.d("TOTAL_FILES",Integer.toString(files.length));

        Log.d("DELETE", "File " + selection + " is to be deletedfrom " + myPort);
        int delCount = 0;
        for(File file:files){
            Log.d("FILENAME",file.getName());
            if (file.exists()) {
                file.delete();
                delCount++;
            }
            Log.d("DELETE", " Deletion completed. " + delCount + " row(s) affected");
        }
        String path=getContext().getFilesDir().getAbsolutePath()+"/"+selection;
        File file = new File ( path );
        if ( file.exists() )
        {
           file.delete();
        }

        return delCount;
    }


    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public static void insertHelper(int hashIdx, String key, String value) {
        AsyncTask owner = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(hashIdx), key, value, "Own");
        Log.d("REPLICATE", "writing the file " + key + " to the node " + ORDER[hashIdx]);

        AsyncTask successor1 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(hashIdx), key, value, "Succ1");
        Log.d("REPLICATE", "writing the file " + key + " to the node " + SUCC1[hashIdx]);

        AsyncTask successor2 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(hashIdx), key, value, "Succ2");
        Log.d("REPLICATE", "writing the file " + key + " to the node " + SUCC2[hashIdx]);

//        return (owner.getStatus()==AsyncTask.Status.FINISHED) && (successor1.getStatus()==AsyncTask.Status.FINISHED) && (successor2.getStatus()==AsyncTask.Status.FINISHED);
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        while (lock){}

        try {
            Log.d("INSERT_METHOD", "insert called");
            // Get the key and value from ContentValues object
            String key = (String) values.get("key");
            String value = (String) values.get("value");

            String hashedKey = genHash(key);
            for (int hashIdx = 0; hashIdx < ORDER.length; hashIdx++) {
                Log.d("HASHED_NOT_KEY", key);
                Log.d("HASHED_KEY", hashedKey);
                Log.d("HASHED_NODE", genHash(ORDER[hashIdx]));
                Log.d("COMPARE", Integer.toString(hashedKey.compareTo(genHash(ORDER[hashIdx]))));
                if (hashedKey.compareTo(genHash(ORDER[hashIdx])) <= 0) {
                    Log.d("CHECK_CASE", "Key " + key + " " + hashedKey + " in IF Block");

                    Log.d("WHERE_DO_YOU_GO", ORDER[hashIdx]);
                    Log.d("WHERE_DO_YOU_GO", SUCC1[hashIdx]);
                    Log.d("WHERE_DO_YOU_GO", SUCC2[hashIdx]);
                    insertHelper(hashIdx, key, value);

                    break;
                } else if (hashedKey.compareTo(genHash(ORDER[ORDER.length - 1])) > 0) {
                    Log.d("CHECK_CASE", "Key " + key + " " + hashedKey + "in ELSE Block");
                    Log.d("WHERE_DO_YOU_GO_CO", ORDER[0]);
                    Log.d("WHERE_DO_YOU_GO_CO", SUCC1[0]);
                    Log.d("WHERE_DO_YOU_GO_CO", SUCC2[0]);
                    insertHelper(0, key, value);

                    break;
                }
            }
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }

        // Write the value to a file named by the key
        Log.v("insert", values.toString());
        return uri;
        // return null;
    }


    public Object[] getFiles(String selection) {
        FileInputStream fileInputStream;
        try {
            fileInputStream = getContext().openFileInput(selection);

            // Read the contents of the file
            // Reference : https://stackoverflow.com/questions/14768191/how-do-i-read-the-file-content-from-the-internal-storage-android-app
            InputStreamReader isr = new InputStreamReader(fileInputStream);
            BufferedReader bufferedReader = new BufferedReader(isr);
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                sb.append(line);
            }

            Object[] keyValuesToInsert = {selection, sb};
            return keyValuesToInsert;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        String[] recAck = new String[3];
        while (lock){}
        String value = "";

        // Create a matrixCursor object to store the keyValue pair
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});

        if (selection.equals("@")) {
            File[] files = getContext().getFilesDir().listFiles();
            for (File file : files) {
                {
                    Log.d("QUERY_FILE", " Dumping " + file.getName() + " with value " + getFiles(file.getName())[1] + " at " + myPort);
                }
                Object[] keyValuesToInsert = getFiles(file.getName());
                matrixCursor.addRow(keyValuesToInsert);
            }

        } else if (selection.equals("*")) {
            StringBuilder keyValueData = new StringBuilder();
            try {
                for (String node : ORDER) {
                    try {
                        Socket getAllLocalData = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(node) * 2);
                        Log.d("PORT_TO_SEND", "Sending the * query to " + node);
                        OutputStream outStream = getAllLocalData.getOutputStream();
                        DataOutputStream outDataStream = new DataOutputStream(outStream);
                        outDataStream.writeUTF("QUERY_ALL");
                        outDataStream.flush();

                        InputStream inStream = getAllLocalData.getInputStream();
                        DataInputStream inDataStream = new DataInputStream(inStream);
                        String receivedData = inDataStream.readUTF();
                        Log.d("QUERY_INCOMING_DATA", receivedData);
                        keyValueData.append(receivedData);
                    } catch (SocketException e) {
                        continue;
                    } catch (EOFException e){
                        continue;
                    } catch (NullPointerException e){
                        continue;
                    }

                }

                String cleanData = keyValueData.substring(0, keyValueData.length() - 2);

                Log.d("CLEANED_DATA", cleanData);
                String[] data = cleanData.split("-");

                for (String instance : data) {
                    Log.d("QUERY_ALL_INSTANCE", instance);
                    if (instance.equals("QUERY_ALL") || instance.equals("EntryPoint:") || instance.equals("")) {
                    } else {
                        for (String instance1 : instance.split("::")) {
                            Log.d("DATA_QUERIED", instance1);
                            if (instance1.contains(":") && !(instance1.contains("QUERY_ALL"))) {
                                String m_key = instance1.split(":")[0];
                                String m_value = instance1.split(":")[1];
                                globalDataCursor.addRow(new Object[]{m_key, m_value});
                                Log.d("QUERY_DATA_COUNT lower", Integer.toString(globalDataCursor.getCount()));
                            }
                        }
                    }
                }
                matrixCursor = globalDataCursor;
                Log.d("QUERY_DATA_COUNT", Integer.toString(globalDataCursor.getCount()));
                globalDataCursor = new MatrixCursor(new String[]{"key", "value"});
            } catch (IOException e) {
                e.printStackTrace();
            }

        } else {
            try {
                String hashedKey = genHash(selection);
                Log.d("HASHED_KEY_BEFORE", hashedKey);
                int currentVersion = 0;

                for (int hashIdx = 0; hashIdx < ORDER.length; hashIdx++) {

                    Log.d("HASHED_NOT_KEY", selection);
                    Log.d("HASHED_KEY", hashedKey);
                    Log.d("HASHED_NODE", genHash(ORDER[hashIdx]));
                    Log.d("COMPARE", Integer.toString(hashedKey.compareTo(genHash(ORDER[hashIdx]))));

                    if (hashedKey.compareTo(genHash(ORDER[hashIdx])) <= 0 && hashedKey.compareTo(genHash(PRED1[hashIdx])) > 0) {


                        int ackIdx = 0;

                        for (String port : new String[]{ORDER[hashIdx], SUCC1[hashIdx], SUCC2[hashIdx]}) {
                            Log.d("QUERY_REQ", "Connecting to " + port);
                            try {
                                Socket getData = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(port) * 2);

                                OutputStream outStream = getData.getOutputStream();
                                DataOutputStream outDataStream = new DataOutputStream(outStream);
                                outDataStream.writeUTF("Q_REQ:" + selection);
                                outDataStream.flush();
                                InputStream inStream = getData.getInputStream();
                                DataInputStream inDataStream = new DataInputStream(inStream);
                                String receivedData = inDataStream.readUTF();
                                Log.d("VALUE_B4_INS", receivedData);
                                Log.d("VALUE_RESP_ELSE", "KEY:" + selection + "  VALUE:" + receivedData.split(":")[3]);

                                if (!receivedData.isEmpty()) {
                                    getData.close();
                                }

                                recAck[ackIdx] = receivedData.split(":")[3];
                                for (String rec : recAck) {
                                    Log.d("REC_CHECK", Boolean.toString(rec == null));
                                }
//                            int receivedVersion = Integer.parseInt(receivedData.split(":")[4]);
                                Log.d("RECEIVED_VALUES", "Message:" + receivedData);

                            } catch (SocketException e) {
                                Log.e("Socket Exception ", "was caught");
                                recAck[ackIdx] = "";
                                continue;
                            } catch (NullPointerException e) {
                                Log.e("NPE ", "was caught");
                                recAck[ackIdx] = "";
                                continue;
                            } catch (EOFException e) {
                                Log.e("EOFE ", "was caught");
                                recAck[ackIdx] = "";
                                continue;
                            }
                            ackIdx++;
                            Log.d("ACK_INDEX", Integer.toString(ackIdx));
                        }

                        break;
                    } else if ((hashedKey.compareTo(genHash(ORDER[ORDER.length - 1])) > 0) || (hashedKey.compareTo(genHash(ORDER[0])) < 0)) {

                        int ackIdx = 0;

                        for (String port : new String[]{ORDER[0], SUCC1[0], SUCC2[0]}) {
                            try {
                                Socket getData = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(port) * 2);

                                OutputStream outStream = getData.getOutputStream();
                                DataOutputStream outDataStream = new DataOutputStream(outStream);
                                outDataStream.writeUTF("Q_REQ:" + selection);
                                outDataStream.flush();
                                InputStream inStream = getData.getInputStream();
                                DataInputStream inDataStream = new DataInputStream(inStream);
                                String receivedData = inDataStream.readUTF();
                                Log.d("VALUE_B4_INS", receivedData);
                                Log.d("VALUE_RESP_ELSE", "KEY:" + selection + "  VALUE:" + receivedData.split(":")[3]);

                                if (!receivedData.isEmpty()) {
                                    getData.close();
                                }

                                recAck[ackIdx] = receivedData.split(":")[3];
                                for (String rec : recAck) {
                                    Log.d("REC_CHECK", Boolean.toString(rec == null));
                                }
//                            int receivedVersion = Integer.parseInt(receivedData.split(":")[4]);
                                Log.d("RECEIVED_VALUES", "Message:" + receivedData);

                            } catch (SocketException e) {
                                Log.e("Socket Exception ", "was caught");
                                recAck[ackIdx] = "";
                                continue;
                            } catch (NullPointerException e) {
                                Log.e("NPE ", "was caught");
                                recAck[ackIdx] = "";
                                continue;
                            } catch (EOFException e) {
                                Log.e("EOFE ", "was caught");
                                recAck[ackIdx] = "";
                                continue;
                            }
//                            value = recAck[ackIdx];
//                            if (currentVersion < receivedVersion) {
//                                Log.d("RECEIVED_LOG_IF0", value);
//                                value = recAck[ackIdx];
//                                Log.d("RECEIVED_LOG_IF1", value);
//                                currentVersion = receivedVersion;
//                            }
                            ackIdx++;
                            Log.d("ACK_INDEX", Integer.toString(ackIdx));
                        }
                        break;
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (String val : recAck) {
                if (val != null || !val.equals("")) {
                    Log.d("EMPTY", "No");
                    value = val;
                    break;
                } else {
                    Log.d("MIRACULOUS_CASE", selection);
                }
            }
            Log.d("VALUE_ON_EXIT", "KEY:" + selection + "  VALUE:" + value);
            Object[] keyValuesToInsert = new Object[]{selection, value};
            matrixCursor.addRow(keyValuesToInsert);
        }
        return matrixCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            ContentValues keyValuesToInsert = new ContentValues();
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
            uriBuilder.scheme("content");
            Uri mUri = uriBuilder.build();

            try {
                // The server listens to a connection request indefinitely.
                while (true) {
                    //Spawn a client socket for an accepted connection
                    Log.d("SERVERTASK", "Waiting for socket");
                    Socket clientSocket = serverSocket.accept();
                    clientSocket.setTcpNoDelay(true);
                    Log.d("SERVERTASK", "Socket accepted");

                    while (lock){}
                    /* This block receives the bytestream sent by the client
                     * and converts it into String object.
                     * Reference: https://docs.oracle.com/javase/8/docs/api/java/io/DataInputStream.html*/

                    InputStream inStream = clientSocket.getInputStream();
                    DataInputStream inDataStream = new DataInputStream(inStream);
                    String receivedMsg = inDataStream.readUTF();
                    Log.d("INIT STRING", receivedMsg);

                    /* The following block checks the header of the message.
                     * I am using headers to distinguish between different requests
                     * of nodes.
                     * The headers and their meanings are described below:
                     *
                     * CONN_REQ : connection request. Used when a new node joins the network
                     * INS_REQ : insert request. This is passed from the predecessor node
                     *           to current node if the predecessor cannot insert the file to
                     *           its storage.
                     * Q_REQ: query request. This is passed from the predecessor node to the
                     *        current node if the predecessor has not stored the queried file
                     * */

                    // ------------------Insertion block------------------------------
                    if (receivedMsg.contains("INS_REQ:")) {
                        FileOutputStream outputStream;
                        String key = receivedMsg.split(":")[1];
                        String value = receivedMsg.split(":")[2];
                        String from = receivedMsg.split(":")[3];
                        Log.d("FROM", from);
                        // Create a keyValue pair to be pushed into the contentResolver object
                        keyValuesToInsert.put("key", key);
                        keyValuesToInsert.put("value", value);
//                        Log.d("INSERT_REQ", "Received key<" + genHash(receivedMsg.split(":")[1]) + "> to be written at " + myPort);
                        // This block builds the URI from predefined scheme and authority
                        Log.d("INSERT_FILE", "File being written:   " + key + " with value:   " + value + "  at node: " + myPort);

                        if (keyVersionTracker.containsKey(key) && !keyVersionTracker.get(key).equals(value)) {
                            String val = keyVersionTracker.get(key);
                            Log.d("VERSIONING", val);
                            int ver = Integer.parseInt(val.split(":")[1]);
                            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                            outputStream.write(value.getBytes());
                            keyVersionTracker.put(key, value + ":" + Integer.toString(ver + 1));
                            Log.d("FILE_VERSION", Integer.toString(ver));
                        } else {
                            keyVersionTracker.put(key, value + ":" + 1);
                            Log.d("FILE_VERSIONING_VALUE", key + "      " + keyVersionTracker.get(key));
                            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                            outputStream.write(value.getBytes());
                        }

                        if (from.equals("OWN") && !ownData.contains(key)) {
                            ownData.add(key);
                        } else if (from.equals("SUCC1") && !pred1Data.contains(key)) {
                            pred1Data.add(key);
                        } else if (from.equals("SUCC2") && !pred2Data.contains(key)) {
                            pred2Data.add(key);
                        }

                        clientSocket.close();
                        //Log.d("CLIENT_SOCKET_CLOSED", "yes");
                    }

                    //--------------------Initial Query block--------------------
                    else if (receivedMsg.contains("ANYTHING_FOR_ME")) {
                        String origin = receivedMsg.split(":")[1];
                        StringBuilder dataWriter = new StringBuilder();
                        if (origin.equals("Pred1")) {
                            for (int index = 0; index < pred1Data.size(); index++) {
                                String key = pred1Data.get(index);
                                String value = keyVersionTracker.get(key);//.split(":")[0];
                                dataWriter.append(key + ":" + value + "::");
                                Log.d("DATA_BEING_SENT", dataWriter.toString());
                            }
                        } else if (origin.equals("Pred2")) {
                            for (int index = 0; index < pred2Data.size(); index++) {
                                String key = pred2Data.get(index);
                                String value = keyVersionTracker.get(key);//.split(":")[0];
                                dataWriter.append(key + ":" + value + "::");
                                Log.d("DATA_BEING_SENT", dataWriter.toString());
                            }
                        } else if (origin.equals("Succ1")) {
                            for (int index = 0; index < ownData.size(); index++) {
                                String key = ownData.get(index);
                                String value = keyVersionTracker.get(key);//.split(":")[0];
                                dataWriter.append(key + ":" + value + "::");
                                Log.d("DATA_BEING_SENT", dataWriter.toString());
                            }
                        } else if (origin.equals("Succ2")) {
                            for (int index = 0; index < ownData.size(); index++) {
                                String key = ownData.get(index);
                                String value = keyVersionTracker.get(key);//.split(":")[0];
                                dataWriter.append(key + ":" + value + "::");
                                Log.d("DATA_BEING_SENT", dataWriter.toString());
                            }
                        }
                        OutputStream outStream = clientSocket.getOutputStream();
                        DataOutputStream outDataStream = new DataOutputStream(outStream);
                        outDataStream.writeUTF(dataWriter.toString());
                        outDataStream.flush();
                    }

                    //----------------------Query block--------------------------
                    else if (receivedMsg.contains("Q_REQ")) {
                        Log.d("Q_REQ", "Inside query req");
                        String keyToSearch = receivedMsg.split(":")[1];
                        Log.d("Q_REQ", "Trying to find " + keyToSearch);
                        Object[] data = getFiles(keyToSearch);

                        Log.d("DATA_META", Integer.toString(data.length));
                        OutputStream outStream = clientSocket.getOutputStream();
                        DataOutputStream outDataStream = new DataOutputStream(outStream);
                        outDataStream.writeUTF("DATA:" + data[0].toString() + ":VERSION:" + data[1].toString());
                        outDataStream.flush();
                    } else if (receivedMsg.contains("QUERY_ALL")) {
                        isDone = false;
                        Log.d("STAR", "Query all initiated");

                        Cursor selfData = query(mUri, null, "@", null, null, null);
                        if (selfData != null) {
                            StringBuilder keyValueData = new StringBuilder();
//                            selfData.moveToFirst();
                            int count = 0;
                            while (selfData.moveToNext()) {
                                int keyIdx = selfData.getColumnIndex("key");
                                int valIdx = selfData.getColumnIndex("value");

                                String key = selfData.getString(keyIdx);

                                Log.d("QUERY_STAR_KEY", "The key is " + key);
                                String val = selfData.getString(valIdx);
                                keyValueData.append(key + ":" + val + "::");
                                count++;
                                Log.d("QUERY_STAR_STR", "The grand string is " + keyValueData.toString());
                                Log.d("INDIVIDUAL_QUERY_COUNT", Integer.toString(count));
                            }

                            OutputStream outStream = clientSocket.getOutputStream();
                            DataOutputStream outDataStream = new DataOutputStream(outStream);
                            Log.d("QUERY_STAR_SEND", "Data being sent to socket " + receivedMsg + "-" + keyValueData.toString());
                            if (!keyValueData.toString().isEmpty()) {
                                outDataStream.writeUTF(receivedMsg + "-" + keyValueData.toString());
                            } else {
                                outDataStream.writeUTF(receivedMsg);
                            }
                            outDataStream.flush();
                        }

                    } else {
                        Log.d("DISP", receivedMsg);
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    static class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {
            int hashIdx = Integer.parseInt(strings[0]);
            String key = strings[1];
            String value = strings[2];
            String dest = strings[3];
            Socket sendData;
            Log.d("DESTINATIOM", dest);
            try {
                if (dest.equals("Succ1")) {
                    sendData = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(SUCC1[hashIdx]) * 2);
                    OutputStream outStream = sendData.getOutputStream();
                    DataOutputStream outDataStream = new DataOutputStream(outStream);
                    outDataStream.writeUTF("INS_REQ:" + key + ":" + value + ":" + "SUCC1");
                    outDataStream.flush();
                    Log.d("SOCKET_CONN_SUCC1", SUCC1[hashIdx]);
                } else if (dest.equals("Succ2")) {
                    sendData = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(SUCC2[hashIdx]) * 2);
                    OutputStream outStream = sendData.getOutputStream();
                    DataOutputStream outDataStream = new DataOutputStream(outStream);
                    outDataStream.writeUTF("INS_REQ:" + key + ":" + value + ":" + "SUCC2");
                    outDataStream.flush();
                    Log.d("SOCKET_CONN_SUCC2", SUCC2[hashIdx]);
                } else {
                    sendData = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(ORDER[hashIdx]) * 2);
                    Log.d("SOCKET_CONN_OWN", ORDER[hashIdx]);
                    OutputStream outStream = sendData.getOutputStream();
                    DataOutputStream outDataStream = new DataOutputStream(outStream);
                    outDataStream.writeUTF("INS_REQ:" + key + ":" + value + ":" + "OWN");
                    outDataStream.flush();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }
    }

    class ArchiveTask extends AsyncTask<String, Void, Void> {
        Map<String, Integer> versionTracker = new HashMap<String, Integer>();

        public int getMyIndex(String port) {
            for (int index = 0; index < ORDER.length; index++) {
                if (port.equals(ORDER[index])) {
                    return index;
                }
            }
            return 0;
        }

        public String getArchivedData(String portStr, int mode) {
            Socket getMyData = null;
            String relation = "";
            String targetPort = "";
            String receivedData = "";
            int myPortIdx = getMyIndex(portStr);
            try {

                switch (mode) {
                    case 1:
                        relation = "Succ1";
                        targetPort = PRED1[myPortIdx];
                        break;
                    case 2:
                        relation = "Succ2";
                        targetPort = PRED2[myPortIdx];
                        break;
                    case -1:
                        relation = "Pred1";
                        targetPort = SUCC1[myPortIdx];
                        break;
                    case -2:
                        relation = "Pred2";
                        targetPort = SUCC2[myPortIdx];
                        break;
                }

                getMyData = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(targetPort) * 2);
                Log.d("PORT_TO_SEND", "Sending the query to " + targetPort);

                OutputStream outStream = getMyData.getOutputStream();
                DataOutputStream outDataStream = new DataOutputStream(outStream);
                outDataStream.writeUTF("ANYTHING_FOR_ME:" + relation);
                outDataStream.flush();

                InputStream inStream = getMyData.getInputStream();
                DataInputStream inDataStream = new DataInputStream(inStream);
                receivedData = inDataStream.readUTF();

            } catch (IOException e) {
                e.printStackTrace();
            }
            return receivedData;
        }

        @Override
        protected Void doInBackground(String... strings) {
            lock = true;
            String portStr = strings[0];
//            File[] files = getContext().getFilesDir().listFiles();
//            for (File file : files) {
////                if (!file.getName().equals("hasRestarted")) {
//                    if (getContext().deleteFile(file.getName())) {
//                        Log.d("DELETE", "File " + file.getName() + " deleted successfully");
//                    }
////                }
//            }

//            FileOutputStream outputStream;
//            Uri.Builder uriBuilder = new Uri.Builder();
//            uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
//            uriBuilder.scheme("content");
//            Uri mUri = uriBuilder.build();
//
//            Cursor globalData = query(mUri, null, "*", null, null);
//            Log.d("IS_DATA_PRESENT", Boolean.toString(globalData != null));
//            if (globalData != null) {
////                globalData.moveToFirst();
//                while (globalData.moveToNext()) {
//                    int keyIdx = globalData.getColumnIndex("key");
//                    int valIdx = globalData.getColumnIndex("value");
//
//                    String key = globalData.getString(keyIdx);
//                    Log.d("QUERY_STAR_KEY_REC", "The key is " + key);
//                    String val = globalData.getString(valIdx);
//                    if (isStorable(key, getMyIndex(portStr))) {
//                        try {
//                            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
//                            outputStream.write(val.getBytes());
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//            }


            StringBuilder prevData = new StringBuilder();
            FileOutputStream outputStream;
            // SEnd request to first predecessor
            prevData.append(getArchivedData(portStr, -1));
            // SEnd request to second predecessor
            prevData.append(getArchivedData(portStr, -2));
            // SEnd request to first successor
            prevData.append(getArchivedData(portStr, 1));
            // SEnd request to second successor
            prevData.append(getArchivedData(portStr, 2));

            Log.d("HAS_PRED_DATA", Boolean.toString(prevData.length() > 0));
            if (prevData.length() > 0) {
                String cleanData = prevData.substring(0, prevData.length() - 2);
                String[] data = cleanData.split("::");
                int newVersion = 0;
                int oldVersion = -1;
                for (String instance : data) {
                    Log.d("PREV_INSTANCE", instance);
                    if (instance.contains(":")) {
                        String keyToInsert = instance.split(":")[0];
                        Log.d("KEY_TO_INSERT", keyToInsert);
                        String valToInsert = instance.split(":")[1];
//                        int version = Integer.parseInt(instance.split(":")[2]);
//                        newVersion = Integer.parseInt(instance.split(":")[2]);
                        Log.d("VALUE_TO_INSERT", valToInsert);
                        try {
//                                if(versionTracker.containsKey(keyToInsert)){
//                                    if(versionTracker.get(keyToInsert)<version){
                            Log.d("RECOVERY_INSERT", "Inserting file " + keyToInsert + " in node " + portStr);
                            outputStream = getContext().openFileOutput(keyToInsert, Context.MODE_PRIVATE);
                            Log.d("THe updated value ", valToInsert);
                            outputStream.write(valToInsert.getBytes());
//                                    }
//                                }
//                                else {
//                                    outputStream = getContext().openFileOutput(keyToInsert, Context.MODE_PRIVATE);
//                                    outputStream.write(valToInsert.getBytes());
//                                    Log.d("THe new value ",valToInsert);
//                                    versionTracker.put(keyToInsert,version);
//                                }

//                            if (insertTracker.contains(keyToInsert)) {
//                                if (newVersion > oldVersion) {
//                                    outputStream = getContext().openFileOutput(keyToInsert, Context.MODE_PRIVATE);
//                                    outputStream.write(valToInsert.getBytes());
//                                    oldVersion = newVersion;
//                                }
//                            } else {
//                                outputStream = getContext().openFileOutput(keyToInsert, Context.MODE_PRIVATE);
//                                outputStream.write(valToInsert.getBytes());
//                                oldVersion = 1;
//                                insertTracker.add(keyToInsert);
//                            }

                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            lock = false;
            return null;
        }
    }
}
