/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.obs;

import org.sensorhub.api.resource.ResourceKey;


/**
 * <p>
 * Immutable key object used to index datastreams in data stores.
 * </p>
 *
 * @author Alex Robin
 * @since Nov 3, 2020
 */
public class DataStreamKey extends ResourceKey<DataStreamKey>
{

    public DataStreamKey(long internalID)
    {
        this.internalID = internalID;
    }
}
