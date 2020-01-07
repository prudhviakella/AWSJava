package main.java.kitokilatest;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import main.java.ExceltoElasticsearch;
import main.java.com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;

import static com.google.common.collect.Collections2.orderedPermutations;

public class ExceltoElastisearch_newsfeeds {
    // Adds the interceptor to the Elasticsearch REST client

    private static final String serviceName = "es";
    private static final String region = "us-east-2";
    private static final String aesEndpoint = "search-kitoki-jyikzwreuzjtw4dvjqly6snq4y.us-east-2.es.amazonaws.com"; // e.g. https://search-mydomain.us-west-1.es.amazonaws.com
    private static final String type = "_doc";

    static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

    public static RestHighLevelClient aesClient() {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(serviceName);
        signer.setRegionName(region);
        HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
        return new RestHighLevelClient(RestClient.builder(HttpHost.create(aesEndpoint)).setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)));
    }
    public static String getMetavalue(Document doc,String metatag)
    {
        Element element = null;
        String meta_tag = "";
        element = doc.select("meta[property=og:"+metatag+"]").first();
        if (element != null && !element.attr("content").isEmpty()) {
            meta_tag = element.attr("content");
        }
        element = doc.select(metatag).first();
        if (element != null && !element.text().isEmpty()) {
            meta_tag = element.text();
        }

        element = doc.select("meta[name=" + metatag + "]").first();
        if (element != null && !element.attr("content").isEmpty()) {
            meta_tag = element.attr("content");
        }
        return meta_tag;
    }
    public static String twitter_tags(Document doc,String metatag)
    {
        Element element = null;
        String meta_tag = "";
        element = doc.select("meta[name=twitter:"+metatag+"]").first();
        if (element != null && !element.attr("content").isEmpty()) {
            meta_tag = element.attr("content");
        }
        return meta_tag;
    }
    public static void main(String[] args)
    {
        Logger logger = LoggerFactory.getLogger(ExceltoElasticsearch.class.getName());
        RestHighLevelClient client = aesClient();
        try
        {
            FileInputStream file = new FileInputStream(new File("C:\\MySpace\\serverless\\Kitoki_NewsFeed.xlsx"));

            //Create Workbook instance holding reference to .xlsx file
            XSSFWorkbook workbook = new XSSFWorkbook(file);

            //Get first/desired sheet from the workbook
            XSSFSheet sheet = workbook.getSheetAt(0);

            //Iterate through each rows one by one
            Iterator<Row> rowIterator = sheet.iterator();
            while (rowIterator.hasNext())
            {
                JSONObject obj = null;
                JSONObject autocomplete_obj = null;
                JSONObject autocomplete_subobj = null;
                JSONObject sub_object = null;
                String title = "";
                String description = "";
                String image = "";
                String keyword = "";
                String twitter_title = "";
                String twitter_desc = "";
                String twitter_image = "";
                Element element = null;
                Row row = rowIterator.next();
                //For each row, iterate through all the columns
                if(row.getRowNum() !=0) {
                    Iterator<Cell> cellIterator = row.cellIterator();
                    obj = new JSONObject();
                    sub_object = new JSONObject();
                    autocomplete_obj = new JSONObject();
                    autocomplete_subobj = new JSONObject();
                    Collection<List<String>> perm = null;
                    while (cellIterator.hasNext()) {
                        Cell cell = cellIterator.next();
                        if (cell.getColumnIndex() == 0) {

                            obj.put("source", cell.getStringCellValue());
                        }
                        else if (cell.getColumnIndex() == 1) {

                            obj.put("type", cell.getStringCellValue());
                        }
                        else if (cell.getColumnIndex() == 2) {

                            cell.setCellType(Cell.CELL_TYPE_STRING);
                            obj.put("title", cell.getStringCellValue());
                            //autocomplete_subobj.put("output",cell.getStringCellValue());
                            perm = orderedPermutations(Arrays.asList(cell.getStringCellValue().split(" ")));
                            autocomplete_obj.put("output",cell.getStringCellValue());
                            StringBuilder sb = new StringBuilder();
                            List<String> tmp_list = new ArrayList<String>();
                            for (List<String> val : perm) {
                                String listString = "";
                                listString = String.join(" ",val);
                                tmp_list.add(listString);
                            }
                            autocomplete_subobj.put("input",tmp_list);
                            autocomplete_obj.put("autocomplete",autocomplete_subobj);
                            autocomplete_obj.put("category","newsfeed");
                        }
                        else if (cell.getColumnIndex() == 3) {

                            obj.put("Description", cell.getStringCellValue());
                        }
                        else if (cell.getColumnIndex() == 4) {

                            obj.put("link", cell.getStringCellValue());
                            Document doc = null;
                            if(!cell.getStringCellValue().isEmpty()
                                    && !cell.getStringCellValue().contains("pdf")) {
                                doc = Jsoup.connect(cell.getStringCellValue()).ignoreContentType(true).get();
                                try {
                                   title = getMetavalue(doc,"title");
                                   description = getMetavalue(doc,"description");
                                   image = getMetavalue(doc,"image");
                                   keyword =  getMetavalue(doc,"keywords");
                                   sub_object.put("title",title);
                                   sub_object.put("description",description);
                                   sub_object.put("image_url",image);
                                   sub_object.put("keywords",keyword);
                                   sub_object.put("type","");
                                   obj.put("meta_tags",sub_object);
                                } catch (NullPointerException ex) {
                                }
                            }else{
                                String texfile_link = cell.getStringCellValue();
                                sub_object.put("title","");
                                sub_object.put("description","");
                                sub_object.put("keywords","");
                                if(texfile_link.contains("pdf")){
                                    sub_object.put("image_url",texfile_link);
                                    sub_object.put("type","pdf");
                                }
                                else if(texfile_link.isEmpty()){
                                    sub_object.put("image_url","");
                                    sub_object.put("type","");
                                }
                                obj.put("meta_tags",sub_object);
                            }
                        }
                        else{

                        }
                    }
//                    System.out.println("json objects are"+obj.toJSONString());
//                    IndexRequest ir = new IndexRequest("newsfeed","newsfeed");
//                    ir.source(obj.toJSONString(), XContentType.JSON);
//                    IndexResponse response = client.index(ir, RequestOptions.DEFAULT);
                    System.out.println("object:"+autocomplete_obj.toJSONString());
                    IndexRequest autocomplete_index = new IndexRequest("kitoki_autocomplete_2","kitoki_autocomplete_2");
                    autocomplete_index.source(autocomplete_obj.toJSONString(), XContentType.JSON);
                    IndexResponse response_auto = client.index(autocomplete_index, RequestOptions.DEFAULT);
                    System.out.println("autocomplete index created:" + response_auto.getId());
                    //System.out.println("Index created with id:"+response_auto.getId());
                    obj = null;
                    autocomplete_obj = null;
                    autocomplete_subobj = null;
                }
            }
            file.close();
            client.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
