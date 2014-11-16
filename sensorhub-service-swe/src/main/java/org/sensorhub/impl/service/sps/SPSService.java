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

package org.sensorhub.impl.service.sps;

import org.sensorhub.api.common.IEventListener;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.module.IModuleStateLoader;
import org.sensorhub.api.module.IModuleStateSaver;
import org.sensorhub.api.service.IServiceModule;


/**
 * <p>
 * OGC Sensor Planning Service implementation.
 * The service can manage any of the sensors installed on the SensorHub instance
 * and is configured automatically from the information generated by the sensors
 * interface.
 * </p>
 *
 * <p>Copyright (c) 2013</p>
 * @author Alexandre Robin <alex.robin@sensiasoftware.com>
 * @since Sep 6, 2013
 */
public class SPSService implements IServiceModule<SPSServiceConfig>
{
    SPSServiceConfig config;
    
    
    public SPSService()
    {
        
    }
    
    
    @Override
    public boolean isEnabled()
    {
        return config.enabled;
    }
    
    
    @Override
    public void init(SPSServiceConfig config) throws SensorHubException
    {
        this.config = config;
    }


    @Override
    public void updateConfig(SPSServiceConfig config) throws SensorHubException
    {
        // TODO Auto-generated method stub

    }
    
    
    @Override
    public void start()
    {
        // TODO Auto-generated method stub
    }
    
    
    @Override
    public void stop()
    {
        // TODO Auto-generated method stub
    }


    @Override
    public SPSServiceConfig getConfiguration()
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public String getName()
    {
        return config.name;
    }
    
    
    @Override
    public String getLocalID()
    {
        return config.id;
    }
    

    @Override
    public void saveState(IModuleStateSaver saver) throws SensorHubException
    {
        // TODO Auto-generated method stub
    }


    @Override
    public void loadState(IModuleStateLoader loader) throws SensorHubException
    {
        // TODO Auto-generated method stub
    }


    @Override
    public void cleanup() throws SensorHubException
    {
        // TODO Auto-generated method stub        
    }


    @Override
    public void registerListener(IEventListener listener)
    {
        // TODO Auto-generated method stub        
    }


    @Override
    public void unregisterListener(IEventListener listener)
    {
        // TODO Auto-generated method stub        
    }
}
