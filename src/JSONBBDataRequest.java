import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.json.*;

import com.bloomberglp.blpapi.DuplicateCorrelationIDException;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.ElementIterator;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.InvalidRequestException;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.RequestQueueOverflowException;
import com.bloomberglp.blpapi.Schema;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Schema.Datatype;

/**
 * @author alex derbes xor@bentboolean.com
 *
 * Request data from Bloomberg and encode the results as a JSONObject.
 * 
 * the main() method has three examples
 * 
 * Currently supports ReferenceDataRequests and HistoricalDataRequests, a different session as well
 * as support for subscription data would be required for MarketDataRequests (not hard)
 * 
 * Flow is:
 * Object Instantiated: connects to bloomberg
 * Build a request using either the getHistoricalDataRequest() method or the getReferenceDataRequest() method
 * 		these methods accept a list of tickers, modifiers and overrides (see bloomberg documentation for more information)
 * sendRequest which returns a JSONObject
 */
public class JSONBBDataRequest {
    
	private Session session;
	private Service refDataService;
    private String serverHost = "localhost";
    private int serverPort = 8194;
	
	public static void main(String args[] ) throws Exception
	{
		JSONBBDataRequest jbhr = new JSONBBDataRequest();

		// Example #1 HistoricalDataRequest Get a bunch of IS stuff for Google and Apple for the last 12 quarters
		{
			ArrayList<String> flds = new ArrayList<String>();
			flds.add("SALES_REV_TURN");
			flds.add("GROSS_MARGIN");
			flds.add("IS_COGS_TO_FE_AND_PP_AND_G");
			flds.add("IS_RD_EXPEND");
			flds.add("IS_INTEREST_EXPENSE");
			flds.add("IS_EXPENSE_STOCK_BASED_COMP");
			flds.add("IS_SGA_OTHER_OP_DEPR_OP_MAINT");
			
			flds.add("FS857");
			
			ArrayList<String> tickers = new ArrayList<String>();
			tickers.add("GOOG US EQUITY");
			tickers.add("AAPL US EQUITY");
			
			HashMap<String, String> modifiers = new HashMap<String, String>();
			modifiers.put("periodicityAdjustment", "FISCAL");
			modifiers.put("periodicitySelection", "QUARTERLY");
			modifiers.put("startDate", "-12CQ");
			modifiers.put("endDate", "");
			modifiers.put("maxDataPoints", "100");
			modifiers.put("returnEids", "false");
			
			HashMap<String, String> overrides = new HashMap<String, String>();
			
			Request request = jbhr.getHistoricalDataRequest(tickers, flds, modifiers, overrides);
			JSONArray results = jbhr.sendRequest(request);
			
			System.out.println( results.toString() );
		}
		
		// Example #2 : companies reporting today as well as top ten holders for listed tickers
		{
			ArrayList<String> flds = new ArrayList<String>();
			flds.add("FS857"); // Internet traffic acquisition costs 
			flds.add("EE998"); // companies reporting today
			flds.add("DY651"); // top 10 holders
			
			ArrayList<String> tickers = new ArrayList<String>();
			tickers.add("GOOG US EQUITY");
			tickers.add("YHOO US EQUITY");
			tickers.add("MSFT US EQUITY");
			
			HashMap<String, String> modifiers = new HashMap<String, String>();
			modifiers.put("periodicityAdjustment", "FISCAL");
			modifiers.put("periodicitySelection", "QUARTERLY");
			modifiers.put("startDate", "-12CQ");
			modifiers.put("endDate", "");
			modifiers.put("maxDataPoints", "100");
			modifiers.put("returnEids", "false");
			
			HashMap<String, String> overrides = new HashMap<String, String>();
			
			Request request = jbhr.getHistoricalDataRequest(tickers, flds, modifiers, overrides);
			JSONArray results = jbhr.sendRequest(request);
			
			System.out.println( results.toString(1) );
		}
		
		// Example #3 - users overrides to return BEST_SALES for three quarters hence 
		{
			ArrayList<String> flds = new ArrayList<String>();
			flds.add("BEST_SALES");
			flds.add("EE998"); // companies reporting today
			flds.add("DY651"); // top 10 holders
			
			ArrayList<String> tickers = new ArrayList<String>();
			tickers.add("PALM US EQUITY");
			tickers.add("IBM US EQUITY");
			
			HashMap<String, String> modifiers = new HashMap<String, String>();
			modifiers.put("returnEids", "false");
			
			HashMap<String, String> overrides = new HashMap<String, String>();
			overrides.put("BEST_FPERIOD_OVERRIDE", "+3FQ");

			Request request = jbhr.getReferenceDataRequest(tickers, flds, modifiers, overrides);
			JSONArray results = jbhr.sendRequest(request);
			
			System.out.println( results.toString(1) );
		}
	}
	
	/**
	 * @throws Exception
	 */
	public JSONBBDataRequest() throws Exception
	{
        SessionOptions sessionOptions = new SessionOptions();
        sessionOptions.setServerHost(serverHost);
        sessionOptions.setServerPort(serverPort);

        session = new Session(sessionOptions);
        if (!session.start()) 
        	throw new Exception("Failed to start BB service");

        if (!session.openService("//blp/refdata"))
            throw new Exception("Failed to open //blp/refdata");

        refDataService = session.getService("//blp/refdata");		
	}
	
	/**
	 * @param meBB
	 * @return either a JSONArray or a JSONObject
	 * @throws Exception
	 * 
	 * Convert a Bloomberg element to JSON
	 * 
	 */
	public static Object BBElementToJSON(Element meBB) throws Exception
	{
		Object me = null;
		Datatype elDt = meBB.datatype();
		String myName = meBB.elementDefinition().name().toString();
		
		if( elDt.equals(Schema.Datatype.SEQUENCE ) )
		{
			if( meBB.isArray() )
			{
				JSONArray children = new JSONArray();
				for(int x=0; x<meBB.numValues(); x++)
					children.put(BBElementToJSON(meBB.getValueAsElement(x)));
				return children;		
			}
			else
			{
				JSONObject meJSON = new JSONObject();
				JSONObject children = new JSONObject();
				for(int x=0; x<meBB.numElements(); x++)
					children.put(meBB.getElement(x).elementDefinition().name().toString()
							, BBElementToJSON(meBB.getElement(x)));
				meJSON.put(myName, children);
				return meJSON;
			}
		}	
		else if( elDt.equals(Schema.Datatype.CHOICE))
		{
			if( meBB.isArray() )
			{
				JSONArray children = new JSONArray();
				for(int x=0; x<meBB.numValues(); x++)
					children.put(BBElementToJSON(meBB.getValueAsElement(x)));
				return children;					
			}
			else
			{
				ElementIterator eli = meBB.elementIterator();
				JSONArray children = new JSONArray();
				while( eli.hasNext() )
					children.put(BBElementToJSON(eli.next()));
				return children;
			}
		}
		else if( elDt.equals(Schema.Datatype.INT32) )
		{
			if( meBB.isArray() )
			{
				JSONArray ary = new JSONArray();
				for(int x=0; x<meBB.numValues(); x++)
					ary.put(meBB.getValueAsInt32(x));
				return ary;
			}
			else
				return meBB.getValueAsInt32();
		}
		else if ( elDt.equals(Schema.Datatype.STRING))
		{
			if( meBB.isArray() )
			{
				JSONArray ary = new JSONArray();
				for(int x=0; x<meBB.numValues(); x++)
					ary.put(meBB.getValueAsString(x));
				return ary;
			}
			else
				return meBB.getValueAsString();
		}
		else if ( elDt.equals(Schema.Datatype.FLOAT64))
		{
			if( meBB.isArray() )
			{
				JSONArray ary = new JSONArray();
				for(int x=0; x<meBB.numValues(); x++)
					ary.put(meBB.getValueAsFloat64(x));
				return ary;
			}
			else
				return meBB.getValueAsFloat64();
		}
		else if ( elDt.equals(Schema.Datatype.DATE))
		{
			if( meBB.isArray() )
			{
				JSONArray ary = new JSONArray();
				for(int x=0; x<meBB.numValues(); x++)
					ary.put(meBB.getValueAsDate(x));
				return ary;
			}
			else
				return meBB.getValueAsDate();
		}
		else
		{
			throw new Exception("DataType: " + meBB.datatype() + " is not supported, probably a simple matter of extending the similar code above where this exception was thrown.  did not encode:" + meBB.datatype()) ;
		}
	}
	
	public Request getHistoricalDataRequest(ArrayList<String> ticks, 
    		ArrayList<String> flds, HashMap<String,String> modifiers, 
    		HashMap<String,String> overrides) throws Exception
	{

        Request request = refDataService.createRequest("HistoricalDataRequest");
        Element securities = request.getElement("securities");
        for( String t : ticks)
        	securities.appendValue(t);
        
        Element fields = request.getElement("fields");
        for( String f : flds )
        	fields.appendValue(f);

        for( String key : modifiers.keySet())
        	request.set( key, modifiers.get(key));
        
        Element ovrs = request.getElement("overrides");
        for( String key : overrides.keySet() )
        {
        	Element oe = ovrs.appendElement();
        	oe.setElement("fieldId", key);
        	oe.setElement("value", overrides.get(key));
        }
        return request;
	}
	
    public Request getReferenceDataRequest(ArrayList<String> ticks, 
    		ArrayList<String> flds, HashMap<String,String> modifiers, 
    		HashMap<String,String> overrides) throws Exception
    {

        Request request = refDataService.createRequest("ReferenceDataRequest");
        Element securities = request.getElement("securities");
        for( String t : ticks)
        	securities.appendValue(t);
        
        Element fields = request.getElement("fields");
        for( String f : flds )
        	fields.appendValue(f);

        for( String key : modifiers.keySet())
        	request.set( key, modifiers.get(key));
        
        Element ovrs = request.getElement("overrides");
        for( String key : overrides.keySet() )
        {
        	Element oe = ovrs.appendElement();
        	oe.setElement("fieldId", key);
        	oe.setElement("value", overrides.get(key));
        }
        return request;
    }
    
    public JSONArray sendRequest(Request request) throws Exception
    {
        session.sendRequest(request, null);

        JSONArray ret = new JSONArray();
        while (true) {
            Event event = session.nextEvent();
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) 
            {
                Message msg = msgIter.next();
                ret.put(BBElementToJSON(msg.asElement()) );
            }
            // wait till we get a response, then return
            if (event.eventType() == Event.EventType.RESPONSE) {
                break;
            }
        }
		return ret;
    }
}
    
    
