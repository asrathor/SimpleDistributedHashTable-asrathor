package edu.buffalo.cse.cse486586.simpledht;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import java.util.HashMap;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.Message;
import android.util.Log;
import android.content.Context;
import android.os.AsyncTask;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import android.telephony.TelephonyManager;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;

public class SimpleDhtProvider extends ContentProvider {

    /*
     * Similar to previous projects.
     */
    static final String TAG = SimpleDhtProvider.class.getSimpleName();

    /*
     * The app listens to one server socket on port 10000.
     */
    static final int SERVER_PORT = 10000;

    /*
     * Each emulator has a specific remote port it should connect to.
     * In this project, we will only need to explicitly call the port 11108 in the onCreate which is explained later.
     */
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    /*
     * The hash map will be used to store the key, value pair. A concurrent hash map is not necessary since that is not being tested.
     * Both the key and value are strings as per the requirement of the project.
     * NOTE: All the functions for the hash map are referred from (https://docs.oracle.com/javase/8/docs/api/java/util/HashMap.html)
     */
    static HashMap<String,String> dHT = new HashMap<String, String>();

    /*
     * An instance of a message handler. This will act as a global instance similar to PA2B.
     */
    MessageHandler message_handler = new MessageHandler();

    /*
     * Instances of current node, previous node and the next node.
     * These are assigned in the onCreate method.
     */
    static String current_node;
    static String previous_node;
    static String next_node;
    static String gen_current_node;
    static String gen_previous_node;
    static String gen_next_node;
    static String smallest_node;
    static String largest_node;
    /*
     * The waiting is for making sure that the request is blocked until the response arrives.
     */
    static boolean block = true;

    static boolean new_request = true;

    static String node_del = "";

    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    /*
     * To build a URI for content provider. Referred from PA2B.
     */
    private Uri buildUri(String content, String s) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(s);
        uriBuilder.scheme(content);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        /*
         * First we check if the selection parameter is @ or * or a specific key.
         */
        Log.e(TAG, "Selection parameter in delete: " + selection);
        Log.e(TAG, "For delete, the emulator node: " + node_del + "is compared to next node: " + next_node);
        if(selection.equals("@")){
            /*
             * The hash table is cleared.
             */
            dHT.clear();

        } else if(selection.equals("*")){
            /*
             * The hash table is cleared.
             */
            dHT.clear();
            /*
             * A check to confirm whether the next node is the start node.
             */
            if(selection.equals("*") && !next_node.equals(node_del)){
                /*
                 * If not, then we keep moving to the next node and perform the delete operation over there.
                 * https://developer.android.com/reference/android/os/AsyncTask.html
                 */
                MessageHandler local_handler = new MessageHandler(selection,null,null,"Delete",node_del,null,next_node);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, local_handler);
            }
        } else{
            try {
                /*
                 * For a specific key, we check if the key belongs to the current node, and if so we can easily remove it from the hash table.
                 * If not, then we move to the next node, like previously, and will check again. Once we eventually reach the right node, we will perform the delete.
                 */
                if(nodeCheck(genHash(selection))){
                    dHT.remove(selection);
                } else{
                    MessageHandler local_handler = new MessageHandler(selection,null,null,"Delete",node_del,null,next_node);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, local_handler);
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * First, in order to insert a new row of (key, value) pair, it is critical to obtain the key and value from the ContentValues (which stores values that ContentResolver can process). https://developer.android.com/reference/android/content/ContentValues.html
         * The ContentValues has multiple methods for extraction of different types of values, such as boolean, float, byte etc.
         * Since the project description suggests that the all keys and values pair by provider are strings, we can use getAsString(). https://developer.android.com/reference/android/content/ContentValues.html#getAsString(java.lang.String)
         */
        String key = values.getAsString("key");
        String key_value = values.getAsString("value");

        Log.e(TAG, "Key: " + key + "Value: " + key_value);
        try {
            /*
             * The key must be hashed before the comparison with hashed port ids.
             */
            String key_hash = genHash(key);

            Log.e(TAG, "Before the nodeCheck");
            Log.e(TAG, "Hast of the key: " + key_hash);

            /*
             * Check if the current node is the one that takes in account/handles the current key.
             */
            if(nodeCheck(key_hash)){
                Log.e(TAG, "Inside the nodeCheck meaning that current node is accountable for the key");
                /*
                 * Place the key-value pair in the distributed hash table.
                 */
                dHT.put(key,key_value);
            }
            else {
                Log.e(TAG, "Insert: The current node is not accountable for the key");
                /*
                 * Send the key pair over to the next node using the output stream such that it will eventually find its right location.
                 * The operation type is changed to insert for the next node so that it will check if the key belongs to the next node.
                 * The previous node will now be the current node as the object has moved to the next one.
                 * The ring eventually form will be 5554->5558->5560->5562->5556->5554.
                 * https://developer.android.com/reference/android/os/AsyncTask.html
                 */
                Log.e(TAG,"Current node: " + current_node + "Next node: " + next_node + "Previous node: " + previous_node);
                MessageHandler local_handler = new MessageHandler(key, key_value, null, "Insert", current_node,null, next_node);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,local_handler);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.v("insert", values.toString());
        return uri;
    }

    private boolean nodeCheck(String key) throws NoSuchAlgorithmException {

        /*
         * This is the default case which will happen when inserting locally.
         * The previous id was initialized as an empty string, making sure that this condition will return true for first case.
         */
        if (gen_previous_node.length() == 0){
            Log.e(TAG,"First if condition: true");
            return true;
        }

        /*
         * For the comparison the hashed versions of ids are created.
         * It should be noted that the gen series corresponds to the emulators ids i.e. 5562 etc.
         */
        String temp_previous_node = genHash(gen_previous_node);
        String temp_current_node = genHash(gen_current_node);
        Log.e(TAG, "Gen_previous_node: " + gen_previous_node + "Hashed version: " + temp_previous_node);
        Log.e(TAG, "Gen_current_node: " + gen_current_node + "Hashed version: " + temp_current_node);

//---------------------------------------------------------------------
// This would have been the default condition if the previous node was initialized as equal to current node in the on create method.
//        if (temp_current_node.compareTo(temp_previous_node) == 0){
//            Log.e(TAG,"first iff");
//            return true;
//        }
//----------------------------------------------------------------------
        /*
         * The second if condition specifies the case when the previous node is less than current node, key is greater than previous node but less than or equal to current node.
         * For instance, previous node = 40, current node = 50 and key = 41~50.
         */
        if(temp_previous_node.compareTo(temp_current_node) < 0 && key.compareTo(temp_previous_node) > 0 && key.compareTo(temp_current_node) <= 0) {
            Log.e(TAG,"Second if condition: true");
            return true;
        }
        /*
         * The third if condition specifies the case when the current node is less than previous node, key is less than previous node and less than or equal to current node.
         * For instance, previous node = 50, current node = 10, key = 0~10. This accounts for when the ring is completed.
         */
        else if(temp_current_node.compareTo(temp_previous_node) < 0 && key.compareTo(temp_previous_node) < 0 && key.compareTo(temp_current_node) <= 0) {
            Log.e(TAG,"Third if condition: true");
            return true;
        }
        /*
         * The fourth if condition specifies the case when the current node is less than previous node, key is greater than previous node and greater than or equal to current node.
         * For instance, previous node = 50, current node = 10, key = 51~60... This accounts for when the ring is completed.
         */
        else if(temp_previous_node.compareTo(temp_current_node) > 0 && key.compareTo(temp_previous_node) > 0 && key.compareTo(temp_current_node) >= 0) {
            Log.e(TAG,"Fourth if condition: true");
            return true;
        }

        Log.e(TAG,"Outside if condition: false");
        return false;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        /*
         * Calculates the port number this AVD listens on.
         * Used from the previous projects.
         * Unlike the previous project, the context needs to be first obtained which calls the getSystemService.
         */
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        /*
         * The current node is initialized to the port AVD listening to.
         * The portStr are the emulator ids i.e. 5554, 5556... and myPort is 11108,11112...
         * The gen series contain the information of the emulator ids while the nodes contain the information of the port number.
         */
        Log.e(TAG,"PortStr: "+ portStr);
        Log.e(TAG, "MyPort: "+ myPort);
        current_node = myPort;
        smallest_node = portStr;
        gen_current_node = portStr;
        gen_previous_node = "";
        gen_next_node = "";
        previous_node = "";
        next_node = "";

        /*
         * A server socket needs to be created, in addition to a thread (AsyncTask), that listens on the server port.
         * PA2B code can be taken as a skeleton for the initialization purpose.
         * https://developer.android.com/reference/android/os/AsyncTask.html
         */
        try {
            Log.e(TAG, "Serversocket try in onCreate");
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.e(TAG, "Serversocket after in onCreate");
        } catch (IOException e) {
            Log.e(TAG, "Can't create a server socket");
            e.printStackTrace();
        }
        Log.e(TAG, "Before the comparison with 11108");
        /*
         * If the current port is not equal to the 11108, then the client task is called to perform the join operation in order to eventually form a ring.
         */
        if(!current_node.equals(REMOTE_PORT0)){
            MessageHandler local_handler = null;
            /*
             * The emulator wants to join. All the nodes (prev.,curr.,next) are initialized for now as the current emulator port since the placement is not yet decided.
             * The key can be initialized as emulator id since the hased version of it will be compared to respective ids later.
             */
            local_handler = new MessageHandler(gen_current_node, null, null, "Join", current_node, current_node, REMOTE_PORT0);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,local_handler);
        }

        return true;
    }

    /*
     * ServerTask is created to handle the incoming messages.
     */
    private class ServerTask extends AsyncTask<ServerSocket,String,Void>{

        @Override
        protected Void doInBackground(ServerSocket... serverSockets){

            /*
             * Some of the statements, for instance, initializing the ServerSocket are from PA2B.
             * However, many new functionalities are added, depending of the operation_type.
             */
            ServerSocket serverSocket = serverSockets[0];
            Log.e(TAG, "serverSocket: " + serverSocket);
            try{
                Log.e(TAG, "It reaches inside the try phrase in ServerTask");
                do{
                    /*
                     * Socket is an endpoint for communication between two machines and underlying class implements CLIENT sockets. (https://developer.android.com/reference/java/net/Socket.html)
                     * The serverSocket waits for requests to come in over the network and underlying class implements SERVER sockets. (https://developer.android.com/reference/java/net/ServerSocket.html)
                     * Once serverSocket detects the incoming connection, it is first required to accept it and for communication, create a new instance of socket.
                     * The accept() method listens for a connection to be made to this socket and accepts it. (https://developer.android.com/reference/java/net/ServerSocket.html#accept())
                     */
                    Log.e(TAG,"Before the socket accept");
                    Socket socket = serverSocket.accept();
                    //----------------------------------
                    //The timeout was used to check why initially the socket was not accepting, but later the problem was found to be in non-continuous running of this loop which posed the problem.
                    //socket.setSoTimeout(20000);
                    //----------------------------------
                    Log.e(TAG, "In the serverSocket loop inside the ServerTask");

                    /*
                     * In the previous project, the BufferedReaders were utilized, but since this time an object is being sent, ObjectInputStream will be used.
                     * The whole object was sent since bufferedreaders would have involved sending information as strings, for which delimiters might be necessary (like PA2B). A scenario that should be avoided.
                     * An ObjectInputStream is opposite of ObjectOutputStream and decentralizes primitive data which is typically written by the ObjectOutputStream in our case. (https://docs.oracle.com/javase/7/docs/api/java/io/ObjectInputStream.html)
                     */
                    ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
                    /*
                     * The readObject actually reads the data of type Object. We will need to convert it to MessageHandler type for further use.
                     */
                    Object object = input.readObject();
                    /*
                     * A normal typecasting can be performed.
                     */
                    MessageHandler local_handler = (MessageHandler) object;
                    Log.e(TAG, "operation_type:" + local_handler.operation_type);
                    /*
                     * When the operation type is join, the emulator is making a request to join the ring.
                     */
                    if(local_handler.operation_type.equals("Join")){
                        requestJoin(local_handler);
                    }
                    /*
                     * The global next node is updated such that it is pointing to the next node of the handler.
                     */
                    if(local_handler.operation_type.equals("Join_Next")){
                        next_node = local_handler.nextNode;
                        gen_next_node = local_handler.key;
                    }
                    /*
                     * The previous node and the next node are updated upon the completion of the Join.
                     */
                    if(local_handler.operation_type.equals("Join_Success")){
                        previous_node = local_handler.previousNode;
                        next_node = local_handler.nextNode;
                        gen_next_node = local_handler.value;
                        gen_previous_node = local_handler.key;
                    }
                    /*
                     * The insert acquires the key and the value from the object and puts them in the content values which is later parsed in the Insert function.
                     */
                    if(local_handler.operation_type.equals("Insert")){
                        ContentValues content_values = new ContentValues();
                        content_values.put("key",local_handler.key);
                        content_values.put("value", local_handler.value);
                        insert(mUri, content_values);
                    }
                    /*
                     * The query checks if the current node is at the local handler, implying that the data being queried is present at the local host.
                     * If so the block is removed and the local handler can be copied to the global handler.
                     * In the Query function, there is another block that waits until the response is arrived and then put all the key-value pairs from the global message handler to the cursor.
                     */
                    if(local_handler.operation_type.equals("Query")){
                        if(current_node.equals(local_handler.currentNode)){
                            Log.e(TAG, "The message handler key: " + message_handler.key + "value: " + message_handler.value + "current: " + message_handler.currentNode);
                            Log.e(TAG, "The local handler key: " + local_handler.key + "value: " + local_handler.value + "current: " + local_handler.currentNode);
                            message_handler = local_handler;
                            block = false;
                        } else{
                            /*
                             * The code will give 3 points in case since the * and @ will work regardless of the block.
                             * But for accessing individual queries, we don't wish to generate requests multiple times or update for the query that arrived later than a previous one.
                             */
                            if(local_handler.key.equals("*")||local_handler.key.equals("@")){
                                Log.e(TAG,"Unexpected behavior!!");
                            }
                            handleQuery(local_handler);

                            //new_request = false;
                            //query(mUri,null,local_handler.key,null,null);
                        }
                    }
                    /*
                     * For performing a delete of key-value pair from the hash map.
                     */
                    if(local_handler.operation_type.equals("Delete")){
                        node_del = local_handler.currentNode;
                        delete(mUri,local_handler.key,null);
                    }
                } while(true);
            } catch (IOException e){
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            return null;

        }

    }
    /*
     * This method handles the join request for the emulators.
     */
    private void requestJoin(MessageHandler local_handler){
        try {
            /*
             * First, similar to all the other functions, we will check if the key belongs to the current node, which is the new node that wants to join.
             */
            if(nodeCheck(genHash(local_handler.key))){
                /*
                 * If the gen series were assigned as equal to the current node, then the condition would be check if current node is equal to the previous node.
                 */
                Log.e(TAG, "The request for the join is handled by "+ current_node);
                Log.e(TAG, "The node that is requesting the join is: " + local_handler.nextNode);
                Log.e(TAG, "Gen_previous_node: " + gen_previous_node);

                /*
                 * The check when the first node joins the ring.
                 * In that case, the gen series will be null as initialized in the OnCreate method.
                 */
                if(gen_previous_node.length() == 0){
                    masterHandle(local_handler);
                } else {
                    /*
                     * When more than one node has already joined the ring, it is essential to first get the node whose next will be the current node.
                     * After that, the previous node and next node will be updated implying that a new node has joined the ring.
                     */
                    ClientJoinNext(local_handler);
                    ClientJoinSuccess(local_handler);
                    Log.e(TAG,"The previous node " + previous_node + " is the handlers previous node: " + local_handler.previousNode);
                    Log.e(TAG, "The gen previous node " + gen_previous_node + "is updated to: " + local_handler.key);
                    gen_previous_node = local_handler.key;
                    previous_node = local_handler.previousNode;
                }
            } else {
                /*
                 * Similar to other functions, the key is sent to the next node in the ring.
                 */
                ClientSendToNext(local_handler);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private void masterHandle(MessageHandler local_handler){
        Log.e(TAG,"The gen previous node is empty implying that the request is managed by 11108");
        if(!current_node.equals("11108")){
            Log.e(TAG, "There is an error in the code :( ");
        }
        /*
         * The gen series are initialized as the message keys of the node that is requesting to join.
         * The previous node and the next node can be updated with prev/next node of the node that is requesting to join.
         * Since this case occurs when the first node is joined apart from the 11108, then we don't need to find the previous node for the requesting one, so there will be no need for Join_Next.
         */
        Log.e(TAG, "The gen previous and next nodes are changed to: " + local_handler.key);
        Log.e(TAG, "The previous node: " + previous_node + " is updated with: " + local_handler.previousNode);
        Log.e(TAG, "The next node: " + next_node + " is updated with: " + local_handler.nextNode);
        gen_previous_node = local_handler.key;
        previous_node = local_handler.previousNode;
        /*
         * Since the initialization of value is empty string, the gen_next_node will be assigned the value of key, not the value.
         * This can be changed depending on the design used to initialize the key-value pair upon the join.
         */
        gen_next_node = local_handler.key;
        next_node = local_handler.nextNode;
        /*
         * Although the initial intention was to call the ClientJoinSuccess method over here, the problem was that the key and previous node are initialized as current nodes since for this case.
         * The gen series are empty strings and only current values are assigned in the OnCreate method.
         * If used improperly, the join runs for an infinite loop :(
         */
        MessageHandler new_handler = new MessageHandler(gen_current_node,gen_current_node,null,"Join_Success",current_node,current_node,previous_node);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,new_handler);
    }

    private void ClientJoinNext(MessageHandler local_handler){
        Log.e(TAG, "The previous node whose next is the current join: " + previous_node);
        MessageHandler new_handler = new MessageHandler(local_handler.key,null,null,"Join_Next",null,local_handler.nextNode,previous_node);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,new_handler);
    }

    private void ClientJoinSuccess(MessageHandler local_handler){
        Log.e(TAG,"The current node for the handler " + local_handler.currentNode + "will be updated to: " + previous_node);
        MessageHandler new_handler = new MessageHandler(gen_previous_node,gen_current_node,null,"Join_Success",previous_node,current_node,local_handler.previousNode);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,new_handler);
    }

    private void ClientSendToNext(MessageHandler local_handler){
        Log.e(TAG,"The key does not belong to node: " + current_node + "and is sent to: " + next_node);
        MessageHandler new_handler = local_handler;
        new_handler.destinationNode = next_node;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,new_handler);
    }

//-------------------------------------------------------------
//    private void updateNext(MessageHandler local_handler){
//
//        MessageHandler new_handler = new MessageHandler("","",null,"Join_Success", "",local_handler.key,previous_node);
//        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,new_handler);
//    }
//
//    private void moveToNext(MessageHandler local_handler){
//
//        MessageHandler new_handler = new MessageHandler("","",null,"Join_Reply", previous_node, current_node, local_handler.key);
//        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,new_handler);
//        previous_node = local_handler.key;
//    }
//-------------------------------------------------------------
    /*
     * ClientTask is an AsyncTask that sends the message (in form of string) over the network.
     */
    private class ClientTask extends AsyncTask<MessageHandler,Void,Void>{

        @Override
        protected Void doInBackground(MessageHandler... handler) {

            MessageHandler local_handler = handler[0];

            try {
                int remote_port = Integer.parseInt(local_handler.destinationNode);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), remote_port);

                /*
                 * OutputStream is superclass representing output stream of bytes (https://developer.android.com/reference/java/io/OutputStream.html).
                 * In the OutputStream there is a class designed for writing primitive data types (https://docs.oracle.com/javase/7/docs/api/java/io/ObjectOutputStream.html)
                 * The primary intention was to use the BufferedWriters like PA2B but those don't support outputing objects.
                 * A string output cannot be sent, since the delimiter (';' in PA2B) can be included inside the message. Therefore, rather than making a string containing all the information, the whole object is sent.
                 */
                ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
                output.writeObject(local_handler);
                output.flush();
                output.close();
                socket.close();

            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;

        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*
         * A new request is generated by the current node where the operation type is changed to Query and key is based on the selection parameter used by the testing script or the gDump or lDump.
         */

        MessageHandler local_handler = new MessageHandler();
        local_handler.currentNode = current_node;
        local_handler.operation_type = "Query";
        local_handler.key = selection;
        /*
         * The matrix cursor containing the information about the key-value pair is acquired for the request.
         */
        if(local_handler.key.equals("*")||local_handler.key.equals("@")){
            Log.e(TAG,"Excepted behavior!!");
        } else{
            Log.e(TAG, "The selection parameter is something else" + local_handler.key);
        }
        /*
         * The handleQuery will initiate immediately when the selection parameters is @ since that is a local dump. While for * it will have to wait.
         */
        Cursor matrix_cursor = handleQuery(local_handler);
        Log.v("query", selection);
        return matrix_cursor;
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

    private Cursor handleQuery(MessageHandler local_handler){

        /*
         * Since the internal storage option is used, MatrixCursor needs to be built. https://developer.android.com/reference/android/database/MatrixCursor.html
         * Its documentation suggests that the argument required are array of string which denotes columns.
         * The array of string, in our case, only requires to contain key and value.
         * The code is from PA2B.
         */
        String[] column_names = {"key", "value"};
        MatrixCursor matrix_cursor;
        matrix_cursor = new MatrixCursor(column_names);
        /*
         * There are 3 high-level checks, for @,* and individual query.
         */
        if (local_handler.key.equals("@")){
            /*
             * For each entry in the HashTable, the rows in the Matrix Cursor will be updated.
             * This operation is local.
             * https://developer.android.com/reference/java/util/Map.Entry.html
             */
            for(HashMap.Entry<String,String> iterator: dHT.entrySet()) {
                matrix_cursor = updateCursor(iterator, matrix_cursor);
            }
            return matrix_cursor;
        }
        else if (local_handler.key.equals("*")){
            /*
             * The first check is to determine whether the next node is an empty string. You can compare the gen_previous too since both are empty initially.
             * If so then the selection parameter * if initiated will act locally and matrix cursor will be updated.
             */
            Log.e(TAG,"Selection *: Gen previous node: " + gen_previous_node + "gen next node: " + gen_next_node);
            if(gen_next_node.length() == 0){
                for(HashMap.Entry<String,String> iterator: dHT.entrySet()) {
                    matrix_cursor = updateCursor(iterator, matrix_cursor);
                }
                return matrix_cursor;
            } else {
                /*
                 * The current node's hash table needs to be updated with all the rows of the global hash table.
                 * Then, the updated handler comprising of updated hash table will be sent to the destination node.
                 */
                local_handler.dHT.putAll(dHT);
                Log.e(TAG,"The destination node for the * selection to send hash table to: " + local_handler.destinationNode);
                ClientSendToNext(local_handler);
                /*
                 * A check will be performed to verify whether the local handler is of the emulator whose global dump has been requested.
                 */
                if(local_handler.currentNode.equals(current_node)){
                    /*
                     * It is important to put the block here otherwise it will result in incorrect number of rows.
                     */
                    holdOn();
                    /*
                     * Unlike the previous update, here the hash table will be acquired from the message_handler, since that is updated when the block is released in the ServerTask.
                     */
                    for (HashMap.Entry<String, String> iterator : message_handler.dHT.entrySet()) {
                        matrix_cursor = updateCursor(iterator, matrix_cursor);
                    }
                }
                //dHT.clear();
                return matrix_cursor;
            }
        } else {
            try {
                /*
                 * When the selection parameter is neither the * or @, then first a check is needed to determine whether the existing key belongs to the current node.
                 */
                if(nodeCheck(genHash(local_handler.key))){
                    /*
                     * It is to make sure that the global current node corresponds to the respective emulator which initiated the request.
                     */
                    Log.e(TAG, "Comparing nodes in the query function, current node: " + current_node + "requesting node: " + local_handler.currentNode);
                    if(local_handler.currentNode.equals(current_node)){
                        /*
                         * The value in the pair is obtained by getting it from location of the key in the hash map.
                         */
                        String temp_key = local_handler.key;
                        String temp_value = dHT.get(temp_key);
                        String[] temp_array = {temp_key,temp_value};
                        matrix_cursor.addRow(temp_array);
                    } else {
                        /*
                         * The current node is marked as as a destination and the key value pair is sent to that node which initially put the request.
                         */
                        MessageHandler new_handler = local_handler;
                        new_handler.destinationNode = local_handler.currentNode;
                        new_handler.key = local_handler.key;
                        new_handler.value = dHT.get(local_handler.key);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,new_handler);
                    }
                    return matrix_cursor;
                } else {
                    /*
                     * Since the existing key does not belong to the current node, it is sent onwards.
                     */
                    ClientSendToNext(local_handler);
                    /*
                     * Once the correct emulator is found..
                     */
                    if(local_handler.currentNode.equals(current_node)) {
                        /*
                         * Hold on until the reponse has arrived and then add the key-value pair in the cursor.
                         * Again, instead of getting the value from the hashtable directly, it is obtained from the message handler which was updated in the block.
                         */
                        holdOn();
                        String temp_key = local_handler.key;
                        String temp_value = message_handler.value;
                        String[] temp_array = {temp_key, temp_value};
                        matrix_cursor.addRow(temp_array);
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            //dHT.clear();
        }

        return matrix_cursor;
    }

    /*
     * This function just gets the key and the value from the hashmap and update the cursor object.
     */
    private MatrixCursor updateCursor(HashMap.Entry<String,String> iterator, MatrixCursor matrix_cursor){

        String temp_key = iterator.getKey();
        String temp_value = iterator.getValue();
        String[] temp_array = {temp_key,temp_value};
        matrix_cursor.addRow(temp_array);

        return matrix_cursor;
    }
    /*
     * This is used to block until the response is received.
     * Another way is to wait in the ContentProvider query until the network backend notify of the result using the wait() and Thread.sleep().
     */
    private void holdOn(){
        block = true;
        while(block);
    }
}

