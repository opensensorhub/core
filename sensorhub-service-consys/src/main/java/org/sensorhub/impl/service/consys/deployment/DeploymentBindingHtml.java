 /***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys.deployment;

import static j2html.TagCreator.div;
import java.io.IOException;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.system.IDeploymentWithDesc;
import org.sensorhub.impl.service.consys.resource.RequestContext;
import org.sensorhub.impl.service.consys.sensorml.SmlFeatureBindingHtml;
import j2html.tags.specialized.DivTag;


/**
 * <p>
 * HTML formatter for deployment resources
 * </p>
 *
 * @author Alex Robin
 * @since March 31, 2023
 */
public class DeploymentBindingHtml extends SmlFeatureBindingHtml<IDeploymentWithDesc, IObsSystemDatabase>
{
    final String collectionTitle;
    
    
    public DeploymentBindingHtml(RequestContext ctx, IdEncoders idEncoders, IObsSystemDatabase db, boolean isSummary) throws IOException
    {
        super(ctx, idEncoders, db, isSummary, true);
        this.collectionTitle = "System Deployments";
    }
    
    
    @Override
    protected String getResourceName()
    {
        return "Deployment";
    }
    
    
    @Override
    protected String getCollectionTitle()
    {
        return collectionTitle;
    }
    
    
    @Override
    protected String getResourceUrl(FeatureKey key)
    {
        var procId = idEncoders.getDeploymentIdEncoder().encodeID(key.getInternalID());
        return ctx.getApiRootURL() + "/" + DeploymentHandler.NAMES[0] + "/" + procId;
    }
    
    
    @Override
    protected DivTag getLinks(String resourceUrl, FeatureKey key, IDeploymentWithDesc f)
    {
        return div();
    }
}
