/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.common;


/**
 * <p>
 * Base class for all events that are associated to a particular procedure.<br/>
 * A procedure in SensorHub can be a sensor, a process, etc. but also a group
 * of nested procedures. When using procedure groups, this type of event can
 * be generated by the parent group as well as by group members. 
 * </p>
 *
 * @author Alex Robin
 * @since Apr 23, 2015
 */
public abstract class ProcedureEvent extends Event
{    
    
    public ProcedureEvent(long timeStamp, String procedureID)
    {
        this.timeStamp = timeStamp;
        this.sourceID = procedureID;
    }
    
    
    /**
     * Gets the unique ID of the procedure related to this event.<br/>
     * For procedure groups (e.g. sensor networks), it will be either the ID
     * of the group as a whole (if the event is global) or the ID of a single
     * procedure within the group (if the event applies only to that member)
     * @return Unique ID of related procedure
     */
    public String getProcedureID()
    {
        return sourceID;
    }
    
}
