/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.obs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.sensorhub.api.data.DataStreamInfo;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ResourceParseException;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.system.SystemHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingJson;
import org.vast.data.DataIterator;
import org.vast.swe.SWEConstants;
import org.vast.swe.SWEStaxBindings;
import org.vast.swe.json.SWEJsonStreamReader;
import org.vast.swe.json.SWEJsonStreamWriter;
import org.vast.util.Asserts;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataRecord;
import net.opengis.swe.v20.Vector;


public class DataStreamBindingJson extends ResourceBindingJson<DataStreamKey, IDataStreamInfo>
{
    static final String NO_NAME = "noname";
    
    final String rootURL;
    final SWEStaxBindings sweBindings;
    final IdEncoder sysIdEncoder = new IdEncoder(SystemHandler.EXTERNAL_ID_SEED);
    final Map<String, CustomObsFormat> customFormats;
    SWEJsonStreamReader sweReader;
    SWEJsonStreamWriter sweWriter;
    
    
    DataStreamBindingJson(RequestContext ctx, IdEncoder idEncoder, boolean forReading, Map<String, CustomObsFormat> customFormats) throws IOException
    {
        super(ctx, idEncoder, forReading);
        
        this.rootURL = ctx.getApiRootURL();
        this.sweBindings = new SWEStaxBindings();
        this.customFormats = Asserts.checkNotNull(customFormats);
        
        if (forReading)
            this.sweReader = new SWEJsonStreamReader(reader);
        else
            this.sweWriter = new SWEJsonStreamWriter(writer);
    }
    
    
    @Override
    public IDataStreamInfo deserialize(JsonReader reader) throws IOException
    {
        if (reader.peek() == JsonToken.END_DOCUMENT || !reader.hasNext())
            return null;
                
        String name = null;
        String description = null;
        String outputName = null;
        IDataStreamInfo dsInfo = null;
        
        try
        {
            reader.beginObject();
            while (reader.hasNext())
            {
                var prop = reader.nextName();
                
                if ("name".equals(prop))
                    name = reader.nextString();
                else if ("description".equals(prop))
                    description = reader.nextString();
                else if ("outputName".equals(prop))
                    outputName = reader.nextString();
                else if ("schema".equals(prop))
                {
                    reader.beginObject();
                    
                    // obsFormat must come first!
                    if (!reader.nextName().equals("obsFormat"))
                        throw new ResourceParseException(MISSING_PROP_ERROR_MSG + "schema/obsFormat");
                    var obsFormat = reader.nextString();
                    
                    ResourceBindingJson<DataStreamKey, IDataStreamInfo> schemaBinding = null;
                    if (ResourceFormat.OM_JSON.getMimeType().equals(obsFormat))
                        schemaBinding = new DataStreamSchemaBindingOmJson(ctx, idEncoder, reader);
                    
                    if (schemaBinding == null)
                        throw ServiceErrors.unsupportedFormat(obsFormat);
                    
                    dsInfo = schemaBinding.deserialize(reader);
                }
                else
                    reader.skipValue();
            }
            reader.endObject();
        }
        catch (InvalidRequestException | ResourceParseException e)
        {
            throw e;
        }
        catch (IOException e)
        {
            throw new ResourceParseException(INVALID_JSON_ERROR_MSG + e.getMessage());
        }
        
        // check that mandatory properties have been parsed
        if (outputName == null)
            throw new ResourceParseException(MISSING_PROP_ERROR_MSG + "outputName");
        if (dsInfo == null)
            throw new ResourceParseException(MISSING_PROP_ERROR_MSG + "schema");
        
        // assign outputName to data component
        dsInfo.getRecordStructure().setName(outputName);
        
        // create datastream info object
        dsInfo = DataStreamInfo.Builder.from(dsInfo)
            .withName(name)
            .withDescription(description)
            .build();
        
        return dsInfo;
    }


    @Override
    public void serialize(DataStreamKey key, IDataStreamInfo dsInfo, boolean showLinks, JsonWriter writer) throws IOException
    {
        var publicDsID = encodeID(key.getInternalID());
        var publicSysID = sysIdEncoder.encodeID(dsInfo.getSystemID().getInternalID());
        
        writer.beginObject();
        
        writer.name("id").value(Long.toString(publicDsID, 36));
        writer.name("name").value(dsInfo.getName());
        
        if (dsInfo.getDescription() != null)
            writer.name("description").value(dsInfo.getDescription());
        
        writer.name("system").beginObject()
            .name("id").value(Long.toString(publicSysID, 36))
            .name("outputName").value(dsInfo.getOutputName())
            .endObject();
        
        writer.name("validTime").beginArray()
            .value(dsInfo.getValidTime().begin().toString())
            .value(dsInfo.getValidTime().end().toString())
            .endArray();

        if (dsInfo.getPhenomenonTimeRange() != null)
        {
            writer.name("phenomenonTime").beginArray()
                .value(dsInfo.getPhenomenonTimeRange().begin().toString())
                .value(dsInfo.getPhenomenonTimeRange().end().toString())
                .endArray();
        }

        if (dsInfo.getResultTimeRange() != null)
        {
            writer.name("resultTime").beginArray()
                .value(dsInfo.getResultTimeRange().begin().toString())
                .value(dsInfo.getResultTimeRange().end().toString())
                .endArray();
        }        
                
        if (dsInfo.hasDiscreteResultTimes())
        {
            writer.name("runTimes").beginArray();
            for (var time: dsInfo.getDiscreteResultTimes().keySet())
                writer.value(time.toString());
            writer.endArray();
        }
        
        // observed properties
        writer.name("observedProperties").beginArray();
        for (var obsProp: getObservables(dsInfo))
        {
            writer.beginObject();
            if (obsProp.getDefinition() != null)
                writer.name("definition").value(obsProp.getDefinition());
            if (obsProp.getLabel() != null)
                writer.name("label").value(obsProp.getLabel());
            if (obsProp.getDescription() != null)
                writer.name("description").value(obsProp.getDescription());
            writer.endObject();
        }
        writer.endArray();
        
        // available formats
        writer.name("formats").beginArray();
        writer.value(ResourceFormat.OM_JSON.getMimeType());
        if (ResourceFormat.allowNonBinaryFormat(dsInfo.getRecordEncoding()))
        {
            writer.value(ResourceFormat.SWE_JSON.getMimeType());
            writer.value(ResourceFormat.SWE_TEXT.getMimeType());
            writer.value(ResourceFormat.SWE_XML.getMimeType());
        }
        writer.value(ResourceFormat.SWE_BINARY.getMimeType());
        
        // also list compatible custom formats
        for (var formatEntry: customFormats.entrySet())
        {
            if (formatEntry.getValue().isCompatible(dsInfo))
                writer.value(formatEntry.getKey());
        }
        
        writer.endArray();
        
        if (showLinks)
        {
            var links = new ArrayList<ResourceLink>();
                        
            links.add(new ResourceLink.Builder()
                .rel("system")
                .title("Parent system")
                .href(rootURL +
                      "/" + SystemHandler.NAMES[0] +
                      "/" + Long.toString(publicSysID, 36))
                .build());
            
            links.add(new ResourceLink.Builder()
                .rel("observations")
                .title("Collection of observations")
                .href(rootURL +
                      "/" + DataStreamHandler.NAMES[0] +
                      "/" + Long.toString(publicDsID, 36) +
                      "/" + ObsHandler.NAMES[0])
                .build());
            
            writeLinksAsJson(writer, links);
        }
        
        writer.endObject();
        writer.flush();
    }
    
    
    protected Iterable<DataComponent> getObservables(IDataStreamInfo dsInfo)
    {
        return Iterables.filter(new DataIterator(dsInfo.getRecordStructure()), comp -> {
            var def = comp.getDefinition();
            
            // skip vector coordinates
            if (comp.getParent() != null && comp.getParent() instanceof Vector)
                return false;
            
            // skip data records
            if (comp instanceof DataRecord)
                return false;
            
            // skip well known fields
            if (SWEConstants.DEF_SAMPLING_TIME.equals(def) ||
                SWEConstants.DEF_PHENOMENON_TIME.equals(def) ||
                SWEConstants.DEF_SYSTEM_ID.equals(def))
                return false;
            
            // skip if no metadata was set
            if (Strings.isNullOrEmpty(def) &&
                Strings.isNullOrEmpty(comp.getLabel()) &&
                Strings.isNullOrEmpty(comp.getDescription()))
                return false;
            
            return true;
        });
    }


    @Override
    public void startCollection() throws IOException
    {
        startJsonCollection(writer);        
    }


    @Override
    public void endCollection(Collection<ResourceLink> links) throws IOException
    {
        endJsonCollection(writer, links);        
    }
}
