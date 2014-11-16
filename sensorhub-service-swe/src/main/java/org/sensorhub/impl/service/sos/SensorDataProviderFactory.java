/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
The Initial Developer is Sensia Software LLC. Portions created by the Initial
Developer are Copyright (C) 2014 the Initial Developer. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sos;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.swe.v20.DataArray;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataRecord;
import net.opengis.swe.v20.SimpleComponent;
import org.sensorhub.api.common.Event;
import org.sensorhub.api.common.IEventListener;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.sensor.ISensorDataInterface;
import org.sensorhub.api.sensor.ISensorModule;
import org.sensorhub.api.sensor.SensorException;
import org.sensorhub.impl.SensorHub;
import org.vast.data.DataIterator;
import org.vast.ogc.om.IObservation;
import org.vast.ows.server.SOSDataFilter;
import org.vast.ows.sos.ISOSDataProvider;
import org.vast.ows.sos.SOSOfferingCapabilities;
import org.vast.sweCommon.SWEConstants;
import org.vast.util.DateTime;
import org.vast.util.TimeExtent;


/**
 * <p>
 * Factory for sensor data providers.
 * </p>
 * <p>
 * One data provider factory is created for each offering and is persistent
 * throughout the lifetime of the service, so it must be threadsafe.
 * </p>
 * <p>
 * However, the server obtains a new data provider instance from the factory
 * for each incoming request so the providers themselves don't need to be
 * threadsafe. 
 * </p>
 *
 * <p>Copyright (c) 2013</p>
 * @author Alexandre Robin <alex.robin@sensiasoftware.com>
 * @since Sep 15, 2013
 */
public class SensorDataProviderFactory implements IDataProviderFactory, IEventListener
{
    final SensorDataProviderConfig config;
    final ISensorModule<?> sensor;
        
    
    protected SensorDataProviderFactory(SensorDataProviderConfig config) throws SensorHubException
    {
        this.config = config;
        
        // get handle to sensor instance using sensor manager
        this.sensor = SensorHub.getInstance().getSensorManager().getModuleById(config.sensorID);
        
        // register to module lifecycle events
        SensorHub.getInstance().registerListener(this);
    }
    
    
    @Override
    public SOSOfferingCapabilities generateCapabilities() throws SensorException
    {
        checkEnabled();        
        
        try
        {
            SOSOfferingCapabilities caps = new SOSOfferingCapabilities();
            
            // identifier
            if (config.uri != null)
                caps.setIdentifier(config.uri);
            else
                caps.setIdentifier("baseURL#" + sensor.getLocalID()); // TODO obtain baseURL
            
            // name
            if (config.name != null)
                caps.setTitle(config.name);
            else
                caps.setTitle(sensor.getName());
            
            // description
            if (config.description != null)
                caps.setDescription(config.description);
            else
                caps.setDescription("Data produced by " + sensor.getName());
            
            // observable properties
            List<String> sensorOutputDefs = getObservablePropertiesFromSensor();
            caps.getObservableProperties().addAll(sensorOutputDefs);
            
            // observed area ??
            
            // phenomenon time
            TimeExtent phenTime = new TimeExtent();
            phenTime.setBaseAtNow(true);
            phenTime.setLagTimeDelta(1.0);//sensor.getObservationOutputs().get(0).getAverageSamplingRate());
            caps.getPhenomenonTimes().add(phenTime);
        
            // use sensor uniqueID as procedure ID
            caps.getProcedures().add(sensor.getCurrentSensorDescription().getUniqueIdentifier());
            
            // supported formats
            caps.getResponseFormats().add(SOSOfferingCapabilities.FORMAT_OM2);
            caps.getProcedureFormats().add(SOSOfferingCapabilities.FORMAT_SML2);
            
            // foi types
            
            // obs types
            List<String> obsTypes = getObservationTypesFromSensor();
            caps.getObservationTypes().addAll(obsTypes);
            
            return caps;
        }
        catch (SensorException e)
        {
            throw new RuntimeException("Error while generating capabilities for sensor " + sensor.getName() + " (" + sensor.getLocalID() + ")");
        }
    }
    
    
    protected List<String> getObservablePropertiesFromSensor() throws SensorException
    {
        List<String> observableUris = new ArrayList<String>();
        
        // process only selected outputs
        for (Entry<String, ? extends ISensorDataInterface> entry: sensor.getAllOutputs().entrySet())
        {
            ISensorDataInterface output = entry.getValue();
            if (Arrays.binarySearch(config.hiddenOutputs, entry.getKey()) >= 0)
                continue;
            
            // iterate through all components and add all definition URIs as observables
            // this way only composite with URI will get added
            DataIterator it = new DataIterator(output.getRecordDescription());
            while (it.hasNext())
            {
                String defUri = (String)it.next().getDefinition();
                if (defUri != null && !defUri.equals(SWEConstants.DEF_SAMPLING_TIME))
                    observableUris.add(defUri);
            }
            
            // TODO we should probably filter out some components such as the time stamp alone, etc.
        }
        
        return observableUris;
    }
    
    
    protected List<String> getObservationTypesFromSensor() throws SensorException
    {
        List<String> obsTypes = new ArrayList<String>();
        obsTypes.add(IObservation.OBS_TYPE_GENERIC);
        
        // process only selected outputs
        for (Entry<String, ? extends ISensorDataInterface> entry: sensor.getAllOutputs().entrySet())
        {
            ISensorDataInterface output = entry.getValue();
            if (Arrays.binarySearch(config.hiddenOutputs, entry.getKey()) >= 0)
                continue;
            
            DataComponent dataStruct = output.getRecordDescription();
            if (dataStruct instanceof SimpleComponent)
                obsTypes.add(IObservation.OBS_TYPE_SCALAR);
            else if (dataStruct instanceof DataRecord)
                obsTypes.add(IObservation.OBS_TYPE_RECORD);
            else if (dataStruct instanceof DataArray)
                obsTypes.add(IObservation.OBS_TYPE_ARRAY);
        }
        
        return obsTypes;
    }
    
    
    @Override
    public AbstractProcess generateSensorMLDescription(DateTime t) throws SensorException
    {
        checkEnabled();
        
        if (t == null)
            return sensor.getCurrentSensorDescription();
        else
            return sensor.getSensorDescription(t);
    }

    
    @Override
    public ISOSDataProvider getNewProvider(SOSDataFilter filter) throws SensorException
    {
        checkEnabled();
        return new SensorDataProvider(sensor, filter);
    }
    
    
    /**
     * Checks if provider and underlying sensor are enabled
     * @throws SensorException
     */
    protected void checkEnabled() throws SensorException
    {
        if (!config.enabled)
        {
            String providerName = (config.name != null) ? config.name : "for " + config.sensorID;
            throw new SensorException("Provider " + providerName + " is disabled");
        }
        
        if (!sensor.getConfiguration().enabled)
        {
            throw new SensorException("Sensor " + config.sensorID + " is disabled");
        }
    }


    @Override
    public void handleEvent(Event e)
    {
        /*// we need to enable/disable this provider when the state of the
        // underlying sensor changes
        if (e instanceof ModuleEvent && e.getSource() == sensor)
        {
            if (((ModuleEvent) e).type == ModuleEvent.Type.DELETED)
                config.enabled = false;
            
            if (((ModuleEvent) e).type == ModuleEvent.Type.ENABLED)
                config.enabled = true;
            
            if (((ModuleEvent) e).type == ModuleEvent.Type.DISABLED)
                config.enabled = false;
        }*/       
    }


    @Override
    public void cleanup()
    {
        SensorHub.getInstance().unregisterListener(this);        
    }


    @Override
    public boolean isEnabled()
    {
        return (config.enabled && sensor.getConfiguration().enabled);
    }
}
