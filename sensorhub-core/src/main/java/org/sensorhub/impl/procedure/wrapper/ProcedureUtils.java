/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.procedure.wrapper;

import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.command.ICommandStreamStore;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.impl.procedure.ProcedureTransactionHandler;
import org.vast.data.TextEncodingImpl;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.sensorml.v20.IOPropertyList;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataStream;


/**
 * <p>
 * Utility methods for handling procedure SensorML descriptions
 * </p>
 *
 * @author Alex Robin
 * @date Feb 2, 2021
 */
public class ProcedureUtils
{
    
    public static void addDatastreamsFromOutputs(ProcedureTransactionHandler procHandler, IOPropertyList outputs) throws DataStoreException
    {
        for (var output: outputs)
        {
            if (output instanceof DataStream)
            {
                var ds = (DataStream)output;
                procHandler.addOrUpdateDataStream(ds.getName(), ds.getElementType(), ds.getEncoding());
            }
            else if (output instanceof DataComponent)
            {
                var comp = (DataComponent)output;
                procHandler.addOrUpdateDataStream(comp.getName(), comp, new TextEncodingImpl());
            }
        }
    }
    
    
    public static void addCommandStreamsFromTaskableParams(ProcedureTransactionHandler procHandler, IOPropertyList params) throws DataStoreException
    {
        for (var param: params)
        {
            if (param instanceof DataStream)
            {
                var ds = (DataStream)param;
                procHandler.addOrUpdateCommandStream(ds.getName(), ds.getElementType(), ds.getEncoding());
            }
            else if (param instanceof DataComponent)
            {
                var comp = (DataComponent)param;
                procHandler.addOrUpdateCommandStream(comp.getName(), comp, new TextEncodingImpl());
            }
        }
    }
    
    
    @SuppressWarnings("unchecked")
    public static <T extends AbstractProcess> ProcessWrapper<T> addOutputsFromDatastreams(long procID, T sml, IDataStreamStore dataStreamStore)
    {
        var outputList = new IOPropertyList();
                
        dataStreamStore.select(new DataStreamFilter.Builder()
            .withProcedures(procID)
            .withCurrentVersion()
            .build())
        .forEach(ds -> {
            var output = ds.getRecordStructure();
            outputList.add(ds.getOutputName(), output);
        });
        
        if (sml instanceof ProcessWrapper)
            return ((ProcessWrapper<T>)sml).withOutputs(outputList);
        else
            return ProcessWrapper.getWrapper(sml).withOutputs(outputList);
    }
    
    
    @SuppressWarnings("unchecked")
    public static <T extends AbstractProcess> ProcessWrapper<T> addTaskableParamsFromCommandStreams(long procID, T sml, ICommandStreamStore cmdStreamStore)
    {
        var paramList = new IOPropertyList();
                
        cmdStreamStore.select(new CommandStreamFilter.Builder()
            .withProcedures(procID)
            .withCurrentVersion()
            .build())
        .forEach(cs -> {
            var param = cs.getRecordStructure();
            param.setUpdatable(true);
            paramList.add(cs.getControlInputName(), param);
        });
        
        if (sml instanceof ProcessWrapper)
            return ((ProcessWrapper<T>)sml).withParams(paramList);
        else
            return ProcessWrapper.getWrapper(sml).withParams(paramList);
    }
    
    
    public static <T extends AbstractProcess> ProcessWrapper<T> addIOsFromDataStore(long procID, T sml, IProcedureObsDatabase db)
    {
        var wrapper = addOutputsFromDatastreams(procID, sml, db.getDataStreamStore());
        wrapper = addTaskableParamsFromCommandStreams(procID, sml, db.getCommandStreamStore());
        return wrapper;
    }
}
