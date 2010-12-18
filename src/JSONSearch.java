import java.util.ArrayList;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

class JSONSearchFilter 
{
	public boolean filter(String k, Object v, String term)
	{
		return true;
	}
}


/**
 * @author alex derbes xor@bentboolean.com
 * Quick little helper class to implement something like the DOM's findNodesByName function for a JSONTree.
 * Sure someone has already written something like this that is much better but I couldn't find it.
 */
public class JSONSearch
{
	
	private String term;
	private ArrayList<Object> results = new ArrayList<Object>();
	private JSONSearchFilter filter;
	private void JSONSearch(JSONArray obj) throws JSONException
	{
		for(int x =0; x<obj.length(); x++)
		{
			Object v = obj.get(x);
			JSONSearch(v);
		}
	}
	private boolean filter(String k, Object v)
	{
		if( term.equals(k) && v instanceof JSONObject )
			return true;
		return false;
	}
	private void JSONSearch(JSONObject obj) throws JSONException
	{
		Iterator i = obj.keys();
		while(  i.hasNext() )
		{
			String key = (String) i.next();
			Object v = obj.get(key);
			if( filter(key, v) )
				results.add( v );
			JSONSearch(v);
		}
	}
	private void JSONSearch(Object v) throws JSONException
	{
		if( v instanceof JSONObject )
			JSONSearch( (JSONObject) v );
		if( v instanceof JSONArray )
			JSONSearch( (JSONArray) v );
	}
	public ArrayList<Object> search(String term, Object obj, JSONSearchFilter filter) throws JSONException
	{
		this.filter = filter;		
		this.term = term;
		if( filter == null )
			filter = new JSONSearchFilter();
		JSONSearch(obj);
		return results;
	}
	
}
