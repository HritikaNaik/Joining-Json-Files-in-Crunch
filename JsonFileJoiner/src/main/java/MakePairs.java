import org.json.JSONObject;
import org.json.JSONException;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import com.google.common.base.Splitter;

public class MakePairs extends DoFn<String, Pair<String,String>>{//Since it takes input from a PCollection, and outputs to a PTable
    private static final Splitter SPLITTER = Splitter.onPattern("\\n").omitEmptyStrings();

    @Override
    public void process(String value, Emitter<Pair<String,String>> emitter){
        String featur, id;
        try {
            for(String tuple:SPLITTER.split(value)) { //Take one line at a time
                JSONObject obj = new JSONObject(tuple);//Turn string into json object
                id = obj.getString("product_id");// Extract the id, this sets each apart

                //To ensure that the correct feature goes with the correct field, after being shuffled enroute to the reducer
                if (obj.has("brand"))
                    featur = "0"+obj.getString("brand");
                else if (obj.has("category"))
                    featur = "1"+ obj.getString("category");
                else if (obj.has("category_name"))
                    featur = "2"+ obj.getString("range");
                else
                    featur = "3"+ obj.getString("value");

                emitter.emit(Pair.of(id,featur));//Links product id with associated field, and turns them into a Pair to be sent to the PTable

            }
        } catch (JSONException e){
            System.out.println("Exception");
            e.printStackTrace();
        }
    }

}
