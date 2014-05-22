/*
 * Licensed to Laurent Broudoux (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.lbroudoux.elasticsearch.river.drive.connector;

import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleRefreshTokenRequest;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.Drive.Changes;
import com.google.api.services.drive.Drive.Files;
import com.google.api.services.drive.model.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * This is a connector for querying and retrieving files or folders from
 * a Google Drive. Credentials are mandatory for connecting to remote drive.
 *
 * @author laurent
 */
public class DriveConnector {

    private static final ESLogger logger = Loggers.getLogger(DriveConnector.class);

    /** */
    public static final String APPLICATION_VND_GOOGLE_APPS_FOLDER = "application/vnd.google-apps.folder";
    /** */
    public static final String APPLICATION_VND_GOOGLE_APPS_DOCUMENT = "application/vnd.google-apps.document";
    /** */
    public static final String APPLICATION_VND_GOOGLE_APPS_SPREADSHEET = "application/vnd.google-apps.spreadsheet";

    private final String clientId;
    private final String clientSecret;
    private final String refreshToken;
    private String folderName;
    private Drive service;

    private Set<FolderInfo> subfoldersId = new TreeSet<FolderInfo>();
    private Map<String, String> folderIdToName = new HashMap<String, String>();

    Map<String, String> folderIdToParentId = new HashMap<String, String>();

    private String rootFolderId;

    public DriveConnector(String clientId, String clientSecret, String refreshToken, Client client) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.refreshToken = refreshToken;
    }

    public String getRootFolderId() {
        return rootFolderId;
    }

    public Set<FolderInfo> getSubfoldersId() {
        return subfoldersId;
    }

    public String getFolderName(String folderId) {
        return folderIdToName.get(folderId);
    }


    /**
     * Actually connect to specified drive, exchanging refresh token for an up-to-date
     * set of credentials. If folder name specified, we also retrieve subfolders to scan.
     *
     * @param folderName The name of the root folder to scan.
     */
    public void connectUserDrive(String folderName) throws IOException {
        this.folderName = folderName;
        logger.info("Establishing connection to Google Drive");
        // We'll use some transport and json factory for sure.
        HttpTransport httpTransport = new NetHttpTransport();
        JsonFactory jsonFactory = new JacksonFactory();

        TokenResponse tokenResponse = null;
        try {
            tokenResponse = new GoogleRefreshTokenRequest(httpTransport, jsonFactory, refreshToken, clientId, clientSecret).execute();
        } catch (IOException ioe) {
            logger.error("IOException while refreshing a token request", ioe);
        }

        GoogleCredential credential = new GoogleCredential.Builder()
                .setTransport(httpTransport)
                .setJsonFactory(jsonFactory)
                .setClientSecrets(clientId, clientSecret).build()
                .setFromTokenResponse(tokenResponse);
        //credential.setRefreshToken(refreshToken);

        service = new Drive.Builder(httpTransport, jsonFactory, credential).build();
        logger.info("Connection established.");
    }

    /**
     * Query Google Drive for getting the last changes since the lastChangesId (may be null
     * if this is the first time).
     *
     * @param lastChangesId The identifier of last changes to start from
     * @return A bunch of changes wrapped into a DriveChanges object
     */
    public DriveChanges getChanges(Long lastChangesId) throws IOException {
        if (logger.isInfoEnabled()) {
            logger.info("Getting drive changes since {}", lastChangesId);
        }
        List<Change> result = new ArrayList<Change>();
        Changes.List request = null;

        try {
            // Prepare request object for listing changes.
            request = service.changes().list();
        } catch (IOException ioe) {
            logger.error("IOException while listing changes on drive service", ioe);
        }
        // Filter last changes if provided.
        if (lastChangesId != null) {
            request.setStartChangeId(lastChangesId + 1);
        }

        long largestChangesId = -1;
        do {
            try {
                ChangeList changes = request.execute();
                if (logger.isInfoEnabled()) {
                    logger.info("Found {} items in this changes page", changes.getItems().size());
                    logger.info("  largest changes id is {}", changes.getLargestChangeId());
                }
                // Filter change based on their parent folder.
                for (Change change : changes.getItems()) {
                    if (isChangeInValidSubfolder(change)) {
                        result.add(change);
                    }
                }
                request.setPageToken(changes.getNextPageToken());
                if (changes.getLargestChangeId() > largestChangesId) {
                    largestChangesId = changes.getLargestChangeId();
                }
            } catch (HttpResponseException hre) {
                if (hre.getStatusCode() == 401) {
                    logger.error("Authorization exception while accessing Google Drive");
                }
                logger.info("Token expired. Refresh the credentials.");
                connectUserDrive(folderName);//Let's try to refresh the credentials
                throw hre;
            } catch (IOException ioe) {
                logger.error("An error occurred while processing changes page: " + ioe);
                request.setPageToken(null);
                throw ioe;
            }
        } while (request.getPageToken() != null && request.getPageToken().length() > 0);

        // Wrap results and latest changes id.
        return new DriveChanges(largestChangesId, result);
    }

    /**
     * Download Google Drive file as byte array.
     *
     * @param driveFile The file to download
     * @return This file bytes or null if something goes wrong.
     */
    public byte[] getContent(File driveFile) {
        if (logger.isInfoEnabled()) {
            logger.info("Downloading file content from {}", driveFile.getDownloadUrl());
        }
        // Find an appropriate download url depending on mime type.
        String downloadUrl = getDownloadUrl(driveFile);

        if (downloadUrl != null) {
            InputStream is = null;
            ByteArrayOutputStream bos = null;

            try {
                // Execute GET request on download url and retrieve input and output streams.
                HttpResponse response = service.getRequestFactory()
                        .buildGetRequest(new GenericUrl(downloadUrl))
                        .execute();
                is = response.getContent();
                bos = new ByteArrayOutputStream();

                byte[] buffer = new byte[4096];
                int len = is.read(buffer);
                while (len > 0) {
                    bos.write(buffer, 0, len);
                    len = is.read(buffer);
                }

                // Flush and return result.
                bos.flush();
                return bos.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            } finally {
                if (bos != null) {
                    try {
                        bos.close();
                    } catch (IOException e) {
                    }
                }
                try {
                    is.close();
                } catch (IOException e) {
                }
            }
        } else {
            return null;
        }
    }

    /**
     * Retrieve the download url for a given drive file. Download url can vary
     * depending on file Mime type.
     *
     * @param driveFile the file to get download url for.
     * @return null if no suitable downlad url is found for this dile.
     */
    public String getDownloadUrl(File driveFile) {
        String downloadUrl = null;
        if (driveFile.getDownloadUrl() != null && driveFile.getDownloadUrl().length() > 0) {
            downloadUrl = driveFile.getDownloadUrl();
        } else if (APPLICATION_VND_GOOGLE_APPS_DOCUMENT.equals(driveFile.getMimeType())) {
            downloadUrl = driveFile.getExportLinks().get("application/pdf");
        } else if (APPLICATION_VND_GOOGLE_APPS_SPREADSHEET.equals(driveFile.getMimeType())) {
            downloadUrl = driveFile.getExportLinks().get("application/pdf");
        }
        return downloadUrl;
    }

    /**
     * Retrieve the mime type associated to a Google Drive file.
     *
     * @param driveFile The file to get type for
     * @return This file mime type for indexation
     */
    public String getMimeType(File driveFile) {
        // If document or spreadsheet, we have asked PDF export so tell it's a PDF...
        if (APPLICATION_VND_GOOGLE_APPS_DOCUMENT.equals(driveFile.getMimeType())) {
            return "application/pdf";
        } else if (APPLICATION_VND_GOOGLE_APPS_SPREADSHEET.equals(driveFile.getMimeType())) {
            return "application/pdf";
        }
        return driveFile.getMimeType();
    }

    /** */
    private boolean isChangeInValidSubfolder(Change change) {
        // If no folder specified, change is valid.
        if (folderName == null) {
            return true;
        }
        // Else, check if parent of file changed is in valid subfolders.
        if (change.getFile() != null) {
            List<ParentReference> references = change.getFile().getParents();
            if (references != null && !references.isEmpty()) {
                for (ParentReference reference : references) {
                    for (FolderInfo folderInfo : subfoldersId) {
                        if (folderInfo.getId().equals(reference.getId())) {
                            return true;
                        }
                    }
                    /*
                    if (subfoldersId.contains(reference.getId())) {
                        return true;
                    }*/
                }
            }
        }
        return false;
    }

    /**
     * Retrieve all the ids of subfolders under root folder name (recursively).
     */
    public void buildSubfoldersId() throws IOException {
        Files.List request = null;

        logger.info("Retrieving scanned subfolders under folder {}, this may take a while...", folderName);
        if (folderName == null) {
            logger.info("No root folder found. Exting buildSubfoldersId().");
        }
        // 1st step: ensure folder is existing and retrieve its id.
        try {
            request = service.files().list()
                    .setMaxResults(100)
                    .setQ("title='" + folderName + "' and mimeType='" + APPLICATION_VND_GOOGLE_APPS_FOLDER
                            + "' and 'root' in parents");
            FileList files = request.execute();
            logger.info("Found {} files corresponding to searched root folder", files.getItems().size());
            if (files != null && files.getItems().size() != 1) {
                throw new FileNotFoundException(folderName + " does not seem to be a valid folder into Google Drive root");
            }
            rootFolderId = files.getItems().get(0).getId();
            folderIdToName.put(rootFolderId, files.getItems().get(0).getTitle());
            logger.info("Id of searched root folder is {}", rootFolderId);
        } catch (IOException ioe) {
            logger.error("IOException while retrieving root folder {} on drive service", folderName, ioe);
            throw ioe;
        }

        // 2nd step: retrieve all folders in drive ('cause we cannot get root folder children
        // recursively with a single query) and store them into a map for later filtering.
        try {
            request = service.files().list()
                    .setMaxResults(Integer.MAX_VALUE)
                    .setQ("mimeType='" + APPLICATION_VND_GOOGLE_APPS_FOLDER + "'");
            FileList files = request.execute();
            for (File folder : files.getItems()) {
                logger.info(" file name : " + folder.getOriginalFilename() + ", title : " + folder.getTitle() + ", size : " + folder.getFileSize());
                List<ParentReference> parents = folder.getParents();
                if (parents != null && !parents.isEmpty()) {
                    folderIdToParentId.put(folder.getId(), parents.get(0).getId());
                    folderIdToName.put(folder.getId(), folder.getTitle());
                }
            }
        } catch (IOException ioe) {
            logger.error("IOException while retrieving all folders on drive service", ioe);
            throw ioe;
        }

        // 3rd step: filter folders and store only the ids of children of searched rootFolder.
        for (String folderId : folderIdToParentId.keySet()) {
            // If the root folder, just add it.
            if (folderId.equals(rootFolderId)) {
                subfoldersId.add(new FolderInfo(folderId, folderIdToName.get(folderId)));
            } else {
                // Else, check the parents.
                List<String> parents = collectParents(folderId);
                logger.info("Parents of {} are {}", folderId, parents);
                // Last parent if the root of the drive, so searched root folder is the one before.
                if (parents.size() > 1 && parents.get(parents.size() - 2).equals(rootFolderId)) {
                    // Found a valid path to root folder, add folder and its parents but remove root before.
                    subfoldersId.add(new FolderInfo(folderId, folderIdToName.get(folderId)));
                    parents.remove(parents.size() - 1);
                    for (String parent : parents) {
                        subfoldersId.add(new FolderInfo(parent, folderIdToName.get(parent)));
                    }
                }
            }
        }
        if (logger.isInfoEnabled()) {
            logger.info("Subfolders Id to scan are {}", subfoldersId);

        }
    }

    /**
     * Get the list of parents Id in ascending order.
     */
    public List<String> collectParents(String folderId) {
        String parentId = folderIdToParentId.get(folderId);
        if (logger.isTraceEnabled()) {
            logger.trace("Direct parent of {} is {}", folderId, parentId);
        }
        List<String> ancestors = new ArrayList<String>();
        ancestors.add(parentId);

        if (folderIdToParentId.containsKey(parentId)) {
            ancestors.addAll(collectParents(parentId));
            return ancestors;
        }
        return ancestors;
    }

    public class FolderInfo implements Comparable<FolderInfo> {
        private String Id;
        private String name;

        public FolderInfo(String id, String name) {
            Id = id;
            this.name = name;
        }

        public String getId() {
            return Id;
        }

        public void setId(String id) {
            Id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String toString() {
            return getName();
        }

        @Override
        public int compareTo(FolderInfo o) {
            return getId().compareTo(o.getId());
        }
    }
}
