import java.util.*;
import org.json.JSONObject;
import org.json.JSONException;
import org.apache.crunch.Pair;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

public class JoinFiles extends DoFn<Pair<String,Collection<String>>,String>{
    @Override
    public void process(Pair<String,Collection<String>> P, Emitter<String> emitter){
        try {
            JSONObject obj = new JSONObject();
            obj.put("product_id", P.first());//Extract the id from the key
            String str;

            Iterator<String> it = P.second().iterator();//To iterate over the iterables
            while(it.hasNext()) {//Iterating over the field names in the value
                str = it.next();
                switch(str.charAt(0)){//To ensure that the correct feature goes with the correct field
                    case '0':
                        obj.put("brand", str.substring(1));
                        break;
                    case '1':
                        obj.put("category", str.substring(1));
                        break;
                    case '2':
                        obj.put("category_name", str.substring(1));
                        break;
                    default:
                        obj.put("brand_name", str.substring(1));
                        break;
                }
            }

            emitter.emit(obj.toString());//Finally, turn the json object into a string to output
        } catch (JSONException e){
            System.out.println("Exception");
            e.printStackTrace();
        }
    }

}
