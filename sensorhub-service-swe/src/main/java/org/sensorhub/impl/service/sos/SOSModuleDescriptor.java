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

import org.sensorhub.api.module.IModule;
import org.sensorhub.api.module.IModuleProvider;
import org.sensorhub.api.module.ModuleConfig;


/**
 * <p>
 * Descriptor of SOS service module, needed for automatic discovery by
 * the ModuleRegistry.
 * </p>
 *
 * <p>Copyright (c) 2013</p>
 * @author Alexandre Robin <alex.robin@sensiasoftware.com>
 * @since Sep 7, 2013
 */
public class SOSModuleDescriptor implements IModuleProvider
{

    @Override
    public String getModuleName()
    {
        return "SOS Service";
    }


    @Override
    public String getModuleDescription()
    {
        return "Generic implementation of OGC Sensor Observation Service 2.0";
    }


    @Override
    public String getModuleVersion()
    {
        return "0.5";
    }


    @Override
    public String getProviderName()
    {
        return "Sensia Software LLC";
    }


    @Override
    public Class<? extends IModule<?>> getModuleClass()
    {
        return SOSService.class;
    }


    @Override
    public Class<? extends ModuleConfig> getModuleConfigClass()
    {
        return SOSServiceConfig.class;
    }

}
