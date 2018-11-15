package cc.datafabric.nifi.processors.gcd.service;

import cc.datafabric.nifi.gcd.service.GCDataflowService;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;


@CapabilityDescription("Provides dataflow service")
@Tags({"gcd", "dataflow", "provider"})
public class DefaultGCDataflowService extends AbstractControllerService implements GCDataflowService {

    private Dataflow dataflowService;

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {

        try {
            // Authentication is provided by gcloud tool when running locally
            // and by built-in service accounts when running on GAE, GCE or GKE.
            GoogleCredential credential = GoogleCredential.getApplicationDefault();

            // The createScopedRequired method returns true when running on GAE or a local developer
            // machine. In that case, the desired scopes must be passed in manually. When the code is
            // running in GCE, GKE or a Managed VM, the scopes are pulled from the GCE metadata server.
            // See https://developers.google.com/identity/protocols/application-default-credentials for more information.
            if (credential.createScopedRequired()) {
                credential = credential.createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
            }

            HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

            dataflowService = new Dataflow.Builder(httpTransport, jsonFactory, credential)
                    .setApplicationName("NiFi GCP Dataflow service provider")
                    .build();

        } catch (IOException | GeneralSecurityException e) {
            throw new InitializationException(e);
        }
    }

    @Override
    public Dataflow getDataflowService() {
        return dataflowService;
    }
}
