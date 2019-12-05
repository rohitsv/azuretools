package com.rohitsv.azuretools.adls;

import java.io.*;
import java.nio.file.Files;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;

public class AdlsUploader {

    static Logger logger = LoggerFactory.getLogger(AdlsUploader.class);
    private static final Integer GENERIC_EXCEPTION_EXIT_CODE = -4;
    private static final Integer SUCCESS_EXIT_CODE = 0;

    public static void main(String[] args) throws IOException {

        int exitCode = SUCCESS_EXIT_CODE;

        String dmPropertiesLocation = "C:\\Users\\Rohit\\Documents\\workspace\\azuretools\\src\\main\\resources\\app.properties";
        Properties props = new Properties();
        props.load(new FileReader(dmPropertiesLocation));

        String clientId = props.getProperty("azure.clientid");
        String clientKey = props.getProperty("azure.clientkey");
        String tenantId = props.getProperty("azure.tenantid");
        String authTokenUrl = props.getProperty("azure.authtokenurl");
        String storeName = props.getProperty("azure.adls.storename");
        String accountFQDN = props.getProperty("azure.adls.accountfqdn");
        String adlsFileUploadDir = props.getProperty("azure.adls.fileuploaddir");
        String marketRefOutDir = props.getProperty("tomcat.marketrefoutdir");

        boolean validInput = false;
        if (clientId == null || clientId.isEmpty()) {
            logger.error("clientId cannot be null or empty");
        } else if (clientKey == null || clientKey.isEmpty()) {
            logger.error("clientKey cannot be null or empty");
        } else if (tenantId == null || tenantId.isEmpty()) {
            logger.error("tenantId cannot be null or empty");
        } else if (authTokenUrl == null || authTokenUrl.isEmpty()) {
            logger.error("authTokenUrl cannot be null or empty");
        } else if (storeName == null || storeName.isEmpty()) {
            logger.error("storeName cannot be null or empty");
        } else if (accountFQDN == null || accountFQDN.isEmpty()) {
            logger.error("accountFQDN cannot be null or empty");
        } else if (adlsFileUploadDir == null || adlsFileUploadDir.isEmpty()) {
            logger.error("adlsFileUploadDir cannot be null or empty");
        } else if (marketRefOutDir == null || marketRefOutDir.isEmpty()) {
            logger.error("marketRefOutDir cannot be null or empty");
        } else {
            validInput = true;
        }

        if (validInput) {

            System.out.println("Starting ADLS file upload...");

            // fill in placeholders
            authTokenUrl = authTokenUrl.replace("tenantid", tenantId);
            accountFQDN = accountFQDN.replace("storename", storeName);

            // get authentication token
            AccessTokenProvider provider = new ClientCredsTokenProvider(authTokenUrl, clientId, clientKey);
            ADLStoreClient client = ADLStoreClient.createClient(accountFQDN, provider);

            try {
                // create ADLS root directory if it does not exists
                boolean rootDirExists = client.checkExists(adlsFileUploadDir);
                System.out.println("rootDirExists = " + rootDirExists);
                if (rootDirExists) {
                    System.out.println("clearing old files " + adlsFileUploadDir);
                    boolean oldFilesCleared = client.deleteRecursive(adlsFileUploadDir);
                    System.out.println("old files cleared =  " + oldFilesCleared);
                }
                System.out.println("creating root dir " + adlsFileUploadDir);
                boolean rootDirCreated = client.createDirectory(adlsFileUploadDir);
                System.out.println("rootDirCreated = " + rootDirCreated);

                // upload files
                File fmarketRefOutDir = new File(marketRefOutDir);
                for (File eachSubFolder : fmarketRefOutDir.listFiles()) {
                    if (eachSubFolder.isDirectory()) {
                        for (File eachFile : eachSubFolder.listFiles()) {
                            if (eachFile.isFile()) {
                                String destinationFileName = String.format("%s%s%s%s%s", adlsFileUploadDir,
                                        "/", eachSubFolder.getName(), "/", eachFile.getName());
                                System.out.println("Uploading " + eachFile.getAbsolutePath() + " to " + destinationFileName);
                                ADLFileOutputStream adlsFileOutputStream = client.createFile(destinationFileName,
                                        IfExists.OVERWRITE);
                                Files.copy(eachFile.toPath(), adlsFileOutputStream);
                                adlsFileOutputStream.flush();
                                adlsFileOutputStream.close();
                                System.out.println("Uploaded " + eachFile.getAbsolutePath() + " succesfully to "
                                        + destinationFileName);

                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Exception occurred while copying files to ADLS" + e);
                exitCode = GENERIC_EXCEPTION_EXIT_CODE;
            }
        }
        System.exit(exitCode);
    }
}