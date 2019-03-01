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
 * Immutable base class for all sensor hub events.
 * All sub-classes should remain immutable.
 * </p>
 *
 * @author Alex Robin
 * @param <EventTypeEnum> Enum of possible event sub-type
 * @since Nov 5, 2010
 */
public abstract class Event<EventTypeEnum extends Enum<?>>
{
    protected long timeStamp;
    protected EventTypeEnum type;
    protected String sourceID;
    protected transient Object source;
        
    
    /**
     * @return Time stamp of event creation (Unix time in ms since 1970)
     */
    public long getTimeStamp()
    {
        return timeStamp;
    }
    
    
    /**
     * @return Event sub-type (depending on actual event class)
     */
    public EventTypeEnum getType()
    {
        return type;
    }
    
    
    /**
     * Gets the ID of the source of event.
     * @return ID of the event source
     */
    public String getSourceID()
    {
        return sourceID;
    }
    
    
    /**
     * Gets the source of the event as an object reference.
     * <p><i>Note that this is not guaranteed to be available when using
     * events in a distributed system. Events that are 
     * meant to be used in distributed systems </i></p>
     * @return Source object that generated this event
     */
    public Object getSource()
    {
        return source;
    }
}
