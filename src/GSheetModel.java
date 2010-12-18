import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import org.json.*;

import com.bloomberglp.blpapi.Request;
import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;
import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.SpreadsheetFeed;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.google.gdata.util.AuthenticationException;

public class GSheetModel {

	private static SpreadsheetService service;
	
	
    public ArrayList<WorksheetEntry> getWorkSheet(String name) throws Exception
    {
		URL metafeedUrl = new URL("http://spreadsheets.google.com/feeds/spreadsheets/private/full");
		SpreadsheetFeed feed = service.getFeed(metafeedUrl, SpreadsheetFeed.class);
		ArrayList<WorksheetEntry> ret = new ArrayList<WorksheetEntry>();
		for(SpreadsheetEntry se : feed.getEntries() )
		{
			if( name.equals( se.getTitle().getPlainText()) )
			{
				for(WorksheetEntry we : se.getWorksheets() )
				{
					if( we.getTitle().getPlainText().equals(name))
					ret.add( we );
				}
			}
		}
		return ret;
    }
	
	public GSheetModel(String user, String pass, String id) throws AuthenticationException
	{
		service = new SpreadsheetService(id);
		service.setUserCredentials(user, pass);
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception 
	{

		String gmail_address = "";
		String gmail_password = "";
		String bb_template_name = "bb_template";
		
		JSONBBDataRequest jbr = new JSONBBDataRequest();
		
		ArrayList<String> flds = new ArrayList<String>();
		flds.add("SALES_REV_TURN");
		flds.add("GROSS_MARGIN");
		flds.add("IS_COGS_TO_FE_AND_PP_AND_G");
		flds.add("IS_RD_EXPEND");
		flds.add("IS_INTEREST_EXPENSE");
		flds.add("IS_EXPENSE_STOCK_BASED_COMP");
		flds.add("IS_SGA_OTHER_OP_DEPR_OP_MAINT");
		
		ArrayList<String> tickers = new ArrayList<String>();
		//tickers.add("GOOG US EQUITY");
		tickers.add("MED US EQUITY");
		
		HashMap<String, String> modifiers = new HashMap<String, String>();
		modifiers.put("periodicityAdjustment", "FISCAL");
		modifiers.put("periodicitySelection", "QUARTERLY");
		modifiers.put("startDate", "-12CQ");
		modifiers.put("endDate", "");
		modifiers.put("maxDataPoints", "100");
		modifiers.put("returnEids", "false");
		
		HashMap<String, String> overrides = new HashMap<String, String>();
		
		Request request = jbr.getHistoricalDataRequest(tickers, flds, modifiers, overrides);
		JSONArray results = jbr.sendRequest(request);
		//System.out.println( results.toString(1) );
		
		JSONSearch js = new JSONSearch();
		
		ArrayList<Object> al = js.search("fieldData", results, new JSONSearchFilter()
			{
				public boolean filter(String k, Object v, String term) {
					if( term.equals(k) && v instanceof JSONObject )
						return true;
					return false;
				}
		} );
		
		GSheetModel gsm = new GSheetModel(gmail_address, gmail_password, "random_text");
		ArrayList<WorksheetEntry> wsList = gsm.getWorkSheet(bb_template_name);
		WorksheetEntry ws = wsList.get(0);
		
		ws.setRowCount(50);
		ws.setColCount(al.size() +3);
		ws.update();
		
		ListFeed feed = service.getFeed( ws.getListFeedUrl(), ListFeed.class );
		
		ListEntry header = feed.getEntries().get(0);
		String tags[] = new String[0];
		tags = header.getCustomElements().getTags().toArray(tags);
		
		for( ListEntry entry : feed.getEntries() )
		{
			String bCode = entry.getCustomElements().getValue(tags[0]);
			System.err.println( "ROW " + bCode );
			if( bCode != null && bCode.length() > 0 )
			{
				for(int x=0; x<al.size(); x++ )
				{
					JSONObject rec = (JSONObject) al.get(x);
					String value = "bb:no data";
					if( rec.has( bCode ) )
						value = rec.getString(bCode);
					entry.getCustomElements().setValueLocal(tags[x+2], value);
				}
				ListEntry updatedRow = entry.update();	
			}
			
		}
		
	}
}
