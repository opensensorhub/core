/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.sensor;

import net.opengis.swe.v20.DataBlock;
import org.sensorhub.api.common.IEventHandler;
import org.sensorhub.api.common.IEventListener;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.event.IEventSourceInfo;
import org.sensorhub.api.sensor.ISensor;
import org.sensorhub.api.sensor.ISensorDataInterface;
import org.sensorhub.api.sensor.ISensorModule;
import org.sensorhub.impl.common.BasicEventHandler;
import org.sensorhub.impl.event.EventSourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.util.Asserts;


/**
 * <p>
 * Class providing default implementation of common sensor data interface
 * API methods. By default, storage is unsupported.
 * </p>
 *
 * @author Alex Robin
 * @param <T> Type of parent sensor
 * @since Nov 2, 2014
 */
public abstract class AbstractSensorOutput<T extends IDataProducer> implements ISensorDataInterface
{
    protected static final String ERROR_NO_STORAGE = "Data storage is not supported by driver ";
    protected final T parentSensor;
    protected final IEventHandler eventHandler;
    protected final IEventSourceInfo eventSrcInfo;
    protected final String name;
    protected final Logger log;
    protected DataBlock latestRecord;
    protected long latestRecordTime = Long.MIN_VALUE;
    
    
    public AbstractSensorOutput(ISensorModule<?> parentSensor)
    {
        this(null, parentSensor);
    }
    
    
    /**
     * Constructs a new sensor output with the given name and attached to the
     * provided sensor module.<br/>
     * Events generated by this output will be published using the parent sensor's
     * event handler.
     * @param name output name
     * @param parentSensor parent sensor instance
     */
    @SuppressWarnings("unchecked")
    public AbstractSensorOutput(String name, ISensorModule<?> parentSensor)
    {
        this(name, (T)parentSensor, null);
    }
    
    
    /**
     * Constructs a new sensor output with the given name and attached to the
     * provided sensor module.<br/>
     * Events generated by this output will be published using the provided handler.
     * @param name output name
     * @param parentSensor parent sensor instance
     * @param eventSrcInfo event source info to use when publishing events
     */
    public AbstractSensorOutput(String name, T parentSensor, IEventSourceInfo eventSrcInfo)
    {
        this(name, parentSensor, eventSrcInfo, null);
    }
    
    
    /**
     * Constructs a new sensor output with the given name and attached to the
     * provided sensor module.<br/>
     * @param name
     * @param parentSensor
     * @param eventSrcInfo
     * @param log
     */
    public AbstractSensorOutput(String name, T parentSensor, IEventSourceInfo eventSrcInfo, Logger log)
    {
        this.name = name;
        this.parentSensor = Asserts.checkNotNull(parentSensor, ISensor.class);
        
        // setup event handling stuff
        this.eventHandler = new BasicEventHandler();
        if (eventSrcInfo == null)
        {
            String groupID = parentSensor.getUniqueIdentifier();
            String sourceID = parentSensor.getUniqueIdentifier() + "/outputs/" + getName();
            this.eventSrcInfo = new EventSourceInfo(groupID, sourceID);
        }
        else
            this.eventSrcInfo = eventSrcInfo;
        
        // setup logger
        if (log == null)
            this.log = LoggerFactory.getLogger(getClass().getCanonicalName());
        else
            this.log = log;
    }
    
    
    protected void stop()
    {
        // do nothing by default
    }
    
    
    @Override
    public T getParentProducer()
    {
        return parentSensor;
    }
    
    
    @Override
    public String getName()
    {
        return name;
    }


    @Override
    public boolean isEnabled()
    {
        return true;
    }


    @Override
    public DataBlock getLatestRecord()
    {
        return latestRecord;
    }
    
    
    @Override
    public long getLatestRecordTime()
    {
        return latestRecordTime;
    }
    
    
    @Override
    public void registerListener(IEventListener listener)
    {
        eventHandler.registerListener(listener);
    }


    @Override
    public void unregisterListener(IEventListener listener)
    {
        eventHandler.unregisterListener(listener);
    }
    
    
    @Override
    public IEventSourceInfo getEventSourceInfo()
    {
        return eventSrcInfo;
    }

}
