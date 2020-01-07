package main.java.kitokilatest;


import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import main.java.com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.config.RequestConfig;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;

public class ExceltoElasticsearch {
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
        return new RestHighLevelClient(RestClient.builder(HttpHost.create(aesEndpoint)).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(
                    RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder
                        .setConnectTimeout(5000)
                        .setSocketTimeout(60000);
            }
        }).setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)));
    }
    public static void main(String[] args)
    {
        Logger logger = LoggerFactory.getLogger(ExceltoElasticsearch.class.getName());
        RestHighLevelClient client = aesClient();
        try
        {
            FileInputStream file = new FileInputStream(new File("C:\\MySpace\\serverless\\Kitoki.xlsx"));

            //Create Workbook instance holding reference to .xlsx file
            XSSFWorkbook workbook = new XSSFWorkbook(file);

            //Get first/desired sheet from the workbook
            XSSFSheet sheet = workbook.getSheetAt(2);

            //Iterate through each rows one by one
            Iterator<Row> rowIterator = sheet.iterator();
            while (rowIterator.hasNext())
            {
                JSONObject obj = null;
                JSONObject autocomplete_obj = null;
                Row row = rowIterator.next();
                //For each row, iterate through all the columns
                if(row.getRowNum() !=0) {
                    Iterator<Cell> cellIterator = row.cellIterator();
                    obj = new JSONObject();
                    autocomplete_obj = new JSONObject();
                    while (cellIterator.hasNext()) {
                        Cell cell = cellIterator.next();
                        if (cell.getColumnIndex() == 0) {

                            obj.put("Company", cell.getStringCellValue());
                            autocomplete_obj.put("companyname",cell.getStringCellValue());
                        }
                        else if (cell.getColumnIndex() == 1) {

                            obj.put("Address", cell.getStringCellValue());
                            autocomplete_obj.put("title",cell.getStringCellValue());
                        }
                        else if (cell.getColumnIndex() == 2) {

                            cell.setCellType(Cell.CELL_TYPE_STRING);
                            obj.put("Phone", cell.getStringCellValue());
                        }
                        else if (cell.getColumnIndex() == 3) {

                            obj.put("Fax", cell.getStringCellValue());
                        }
                        else if (cell.getColumnIndex() == 4) {

                            obj.put("E-mail", cell.getStringCellValue());
                        }
                        else if (cell.getColumnIndex() == 5) {

                            obj.put("Contact", cell.getStringCellValue());
                        }
                        else if (cell.getColumnIndex() == 6) {

//                            if (cell.getCellType() != Cell.CELL_TYPE_NUMERIC) {
//                                obj.put("Value (USD)", 0);
//                            } else if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
//                                obj.put("Value (USD)", cell.getNumericCellValue());
//                            }
                            cell.setCellType(Cell.CELL_TYPE_STRING);
                            obj.put("Value (USD)", cell.getStringCellValue());
                        }
                        else{

                        }
                    }
                    System.out.println("json objects are"+obj.toJSONString());
                    IndexRequest ir = new IndexRequest("kitoki_service_partner","kitoki_service_partner");
                    ir.source(obj.toJSONString(), XContentType.JSON);
                    IndexResponse response = client.index(ir, RequestOptions.DEFAULT);
                    System.out.println("Index created with id:"+response.getId());
                    IndexRequest autocomplete_index = new IndexRequest("kitoki_autocomplete","kitoki_autocomplete");
                    autocomplete_index.source(autocomplete_obj.toJSONString(), XContentType.JSON);
                    IndexResponse response_auto = client.index(autocomplete_index, RequestOptions.DEFAULT);
                    System.out.println("autocomplete index created:" + response_auto.getId());
                    System.out.println("Index created with id:"+response.getId());
                    obj = null;
                    autocomplete_obj = null;
                }

            }
            file.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
