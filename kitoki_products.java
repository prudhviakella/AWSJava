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
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Collections2.orderedPermutations;

public class kitoki_products {
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
//        return new RestHighLevelClient(RestClient.builder(HttpHost.create(aesEndpoint)).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
//            @Override
//            public RequestConfig.Builder customizeRequestConfig(
//                    RequestConfig.Builder requestConfigBuilder) {
//                return requestConfigBuilder
//                        .setConnectTimeout(5000)
//                        .setSocketTimeout(60000);
//            }
//        }).setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)));
    }
    public static void main(String args[]) throws IOException {
        Logger logger = LoggerFactory.getLogger(ExceltoElasticsearch.class.getName());
        RestHighLevelClient client = aesClient();
        String xlsx_dir = "C:\\MySpace\\serverless\\products_list\\finetuned\\";
        int Itteration = 0;
        File dir = new File(xlsx_dir);
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File child : directoryListing) {
                System.out.println("Processing file:"+xlsx_dir+child.getName());
                FileInputStream file = new FileInputStream(new File(xlsx_dir+child.getName()));
                String headers_array[];
                XSSFWorkbook workbook = new XSSFWorkbook(file);
                //Get first/desired sheet from the workbook
                XSSFSheet sheet = workbook.getSheetAt(0);
                XSSFRow headers = sheet.getRow(0);
                headers_array = new String[headers.getPhysicalNumberOfCells ()];
                Iterator<Cell> headerIterator = headers.iterator();
                int i = 0;
                while (headerIterator.hasNext()){
                    Cell cell = headerIterator.next();
                    headers_array[i] = cell.getStringCellValue();
                    i++;
                }
                Iterator<Row> rowIterator = sheet.iterator();

                while (rowIterator.hasNext())
                {
                    JSONObject main_obj = null;
                    JSONObject autocomplete = null;
                    JSONObject autocomplete_subobj = null;
                    Row row = rowIterator.next();
                    Collection<List<String>> perm = null;
                    if(row != null) {
                        //For each row, iterate through all the columns
                        if (row.getRowNum() != 0) {
                            main_obj = new JSONObject();
                            autocomplete = new JSONObject();
                            autocomplete_subobj = new JSONObject();
                            List<JSONObject> jsonlist = new ArrayList<>();
                            Iterator<Cell> cellIterator = row.cellIterator();
                            IndexRequest autocomplete_index = null;
                            boolean isrowempty = false;
                            while (cellIterator.hasNext()) {
                                Cell cell = cellIterator.next();
                                if (cell.getColumnIndex() != 0) {
                                    if(cell.getCellType() == Cell.CELL_TYPE_BLANK){
                                        //checking whether row is empty or not if all the cells in the row are blank then
                                        //we can consider row is empty. and isrowempty flag will be false if none of the columns
                                        // in the row has some value
                                    }
                                    else {
                                        isrowempty = true;
                                        cell.setCellType(Cell.CELL_TYPE_STRING);
                                        main_obj.put(headers_array[cell.getColumnIndex()].trim(), cell.getStringCellValue());
                                        if (headers_array[cell.getColumnIndex()].trim().compareTo("Business Details") == 0) {
                                            System.out.println("Itteration:"+Itteration);
                                            //autocomplete.put("title", cell.getStringCellValue());
                                            System.out.println("BusinessDetails:"+cell.getStringCellValue());
                                            String array_business[] = cell.getStringCellValue().split(",");
                                            for(int j=0; j< array_business.length; j++) {
                                                String tmp_arr[] = array_business[j].split(" ");
                                                List<String> tmp_list = new ArrayList<String>();
                                                if(tmp_arr.length < 10) {
                                                    perm = orderedPermutations(Arrays.asList(array_business[j].split(" ")));
                                                    autocomplete.put("output", array_business[j]);
                                                    StringBuilder sb = new StringBuilder();
                                                    int it = 0;
                                                    for (List<String> val : perm) {
                                                        if (it < 2) {
                                                            String listString = "";
                                                            listString = String.join(" ", val);
                                                            tmp_list.add(listString);
                                                            it++;
                                                        }
                                                    }
                                                }
                                                else{
                                                    tmp_list.add(array_business[j]);
                                                }
                                                autocomplete_subobj.put("input", tmp_list);
                                                autocomplete.put("autocomplete_products", autocomplete_subobj);
                                                autocomplete_index = new IndexRequest("kitoki_autocomplete3","kitoki_autocomplete3");
                                                autocomplete_index.source(autocomplete.toJSONString(), XContentType.JSON);
                                                IndexResponse response_auto = client.index(autocomplete_index, RequestOptions.DEFAULT);
                                                System.out.println("autocomplete index created:" + response_auto.getId());
                                                autocomplete_index = null;
                                                Itteration++;
                                            }
                                        }
                                        if (headers_array[cell.getColumnIndex()].trim().compareTo("Company Name") == 0) {
                                            System.out.println("Itteration:"+Itteration);
                                            System.out.println("companyname:"+cell.getStringCellValue());
                                            perm = orderedPermutations(Arrays.asList(cell.getStringCellValue().split(" ")));
                                            autocomplete.put("output",cell.getStringCellValue());
                                            StringBuilder sb = new StringBuilder();
                                            List<String> tmp_list = new ArrayList<String>();
                                            int it = 0;
                                            for (List<String> val : perm) {
                                                if(it < 2) {
                                                    String listString = "";
                                                    listString = String.join(" ", val);
                                                    tmp_list.add(listString);
                                                    it++;
                                                }
                                            }
                                            tmp_list.add(cell.getStringCellValue());
                                            autocomplete_subobj.put("input",tmp_list);
                                            autocomplete.put("autocomplete",autocomplete_subobj);
                                            autocomplete.put("category","product");
                                            autocomplete_index = new IndexRequest("kitoki_autocomplete3","kitoki_autocomplete3");
                                            autocomplete_index.source(autocomplete.toJSONString(), XContentType.JSON);
                                            IndexResponse response_auto = client.index(autocomplete_index, RequestOptions.DEFAULT);
                                            System.out.println("autocomplete index created:" + response_auto.getId());
                                            autocomplete_index = null;
                                            Itteration++;
                                        }
                                    }
                                }
                            }
                            if(isrowempty) {
//                                IndexRequest product = new IndexRequest("kitoki_product","kitoki_product");
//                                product.source(main_obj.toJSONString(), XContentType.JSON);
//                                IndexResponse response = client.index(product, RequestOptions.DEFAULT);
//                                System.out.println("Product Index created with id:"+response.getId());
                                autocomplete = null;
                            }
                            main_obj = null;
                            jsonlist = null;
                            if(Itteration == 181){
                                System.out.println("Recreating connection");
                                client.close();
                                Itteration = 0;
                                client = aesClient();
                            }
                        }
                    }
                }
                System.out.println("file Processed:"+xlsx_dir+child.getName());
                file.close();
                workbook.close();
            }
        }
    }
}
