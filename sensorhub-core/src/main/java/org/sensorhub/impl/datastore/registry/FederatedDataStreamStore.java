/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.registry;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.sensorhub.api.obs.DataStreamFilter;
import org.sensorhub.api.obs.DataStreamKey;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.api.obs.IDataStreamStore;
import org.sensorhub.api.obs.ObsFilter;
import org.sensorhub.api.obs.IDataStreamStore.DataStreamInfoField;
import org.sensorhub.api.procedure.ProcedureFilter;
import org.sensorhub.api.procedure.ProcedureId;
import org.sensorhub.impl.datastore.obs.DataStreamInfoWrapper;
import org.sensorhub.impl.datastore.registry.DefaultDatabaseRegistry.LocalFilterInfo;
import org.vast.util.Asserts;


/**
 * <p>
 * Implementation of datastream store that provides federated read-only access
 * to several underlying databases.
 * </p>
 *
 * @author Alex Robin
 * @date Oct 3, 2019
 */
public class FederatedDataStreamStore extends ReadOnlyDataStore<DataStreamKey, IDataStreamInfo, DataStreamInfoField, DataStreamFilter> implements IDataStreamStore
{
    DefaultDatabaseRegistry registry;
    FederatedObsDatabase db;
    
    
    class DataStreamInfoWithPublicId extends DataStreamInfoWrapper
    {
        ProcedureId publicProcId;        
        
        DataStreamInfoWithPublicId(ProcedureId publicProcId, IDataStreamInfo dsInfo)
        {
            super(dsInfo);
            this.publicProcId = publicProcId;
        }        
        
        @Override
        public ProcedureId getProcedureID()
        {
            return publicProcId;
        }
    }
    
    
    FederatedDataStreamStore(DefaultDatabaseRegistry registry, FederatedObsDatabase db)
    {
        this.registry = registry;
        this.db = db;
    }


    @Override
    public String getDatastoreName()
    {
        return getClass().getSimpleName();
    }


    @Override
    public long getNumRecords()
    {
        long count = 0;
        for (var db: registry.obsDatabases.values())
            count += db.getObservationStore().getDataStreams().getNumRecords();
        return count;
    }
    
    
    protected DataStreamKey ensureDataStreamKey(Object obj)
    {
        Asserts.checkArgument(obj instanceof DataStreamKey, "key must be a DataStreamKey");
        return (DataStreamKey)obj;
    }


    @Override
    public boolean containsKey(Object obj)
    {
        var key = ensureDataStreamKey(obj);
        
        // use public key to lookup database and local key
        var dbInfo = registry.getLocalDbInfo(key.getInternalID());
        if (dbInfo == null)
            return false;
        else
            return dbInfo.db.getObservationStore().getDataStreams().containsKey(new DataStreamKey(dbInfo.entryID));
    }


    @Override
    public boolean containsValue(Object value)
    {
        for (var db: registry.obsDatabases.values())
        {
            if (db.getObservationStore().getDataStreams().containsValue(value))
                return true;
        }
        
        return false;
    }


    @Override
    public IDataStreamInfo get(Object obj)
    {
        var key = ensureDataStreamKey(obj);
        
        // use public key to lookup database and local key
        var dbInfo = registry.getLocalDbInfo(key.getInternalID());
        if (dbInfo != null)
        {
            IDataStreamInfo dsInfo = dbInfo.db.getObservationStore().getDataStreams().get(new DataStreamKey(dbInfo.entryID));
            if (dsInfo != null)
                return toPublicValue(dbInfo.databaseID, dsInfo);
        }
        
        return null;
    }
    
    
    /*
     * Convert to public keys on the way out
     */
    protected DataStreamKey toPublicKey(int databaseID, DataStreamKey k)
    {
        long publicID = registry.getPublicID(databaseID, k.getInternalID());
        return new DataStreamKey(publicID);
    }
    
    
    /*
     * Convert to public values on the way out
     */
    protected IDataStreamInfo toPublicValue(int databaseID, IDataStreamInfo dsInfo)
    {
        long procPublicID = registry.getPublicID(databaseID, dsInfo.getProcedureID().getInternalID());
        ProcedureId publicId = new ProcedureId(procPublicID, dsInfo.getProcedureID().getUniqueID());
        return new DataStreamInfoWithPublicId(publicId, dsInfo);
    }
    
    
    /*
     * Convert to public entries on the way out
     */
    protected Entry<DataStreamKey, IDataStreamInfo> toPublicEntry(int databaseID, Entry<DataStreamKey, IDataStreamInfo> e)
    {
        return new AbstractMap.SimpleEntry<>(
            toPublicKey(databaseID, e.getKey()),
            toPublicValue(databaseID, e.getValue()));
    }
    
    
    /*
     * Get dispatch map according to internal IDs used in filter
     */
    protected Map<Integer, LocalFilterInfo> getFilterDispatchMap(DataStreamFilter filter)
    {
        if (filter.getInternalIDs() != null)
        {
            var filterDispatchMap = registry.getFilterDispatchMap(filter.getInternalIDs());
            for (var filterInfo: filterDispatchMap.values())
            {
                filterInfo.filter = DataStreamFilter.Builder
                    .from(filter)
                    .withInternalIDs(filterInfo.internalIds)
                    .build();
            }
            
            return filterDispatchMap;
        }
        
        else if (filter.getProcedureFilter() != null)
        {
            // delegate to proc store handle procedure filter dispatch map
            var filterDispatchMap = db.procStore.getFilterDispatchMap(filter.getProcedureFilter());
            if (filterDispatchMap != null)
            {
                for (var filterInfo: filterDispatchMap.values())
                {
                    filterInfo.filter = DataStreamFilter.Builder
                        .from(filter)
                        .withProcedures((ProcedureFilter)filterInfo.filter)
                        .build();
                }
            }
            
            return filterDispatchMap;
        }
        
        else if (filter.getObservationFilter() != null)
        {
            // delegate to proc store handle procedure filter dispatch map
            var filterDispatchMap = db.obsStore.getFilterDispatchMap(filter.getObservationFilter());
            if (filterDispatchMap != null)
            {
                for (var filterInfo: filterDispatchMap.values())
                {
                    filterInfo.filter = DataStreamFilter.Builder
                        .from(filter)
                        .withObservations((ObsFilter)filterInfo.filter)
                        .build();
                }
            }
            
            return filterDispatchMap;
        }
        
        return null;
    }
    
    
    @Override
    public Stream<Entry<DataStreamKey, IDataStreamInfo>> selectEntries(DataStreamFilter filter, Set<DataStreamInfoField> fields)
    {
        // if any kind of internal IDs are used, we need to dispatch the correct filter
        // to the corresponding DB so we create this map
        var filterDispatchMap = getFilterDispatchMap(filter);
        
        if (filterDispatchMap != null)
        {
            return filterDispatchMap.values().stream()
                .flatMap(v -> {
                    int dbID = v.databaseID;
                    return v.db.getObservationStore().getDataStreams().selectEntries((DataStreamFilter)v.filter, fields)
                        .map(e -> toPublicEntry(dbID, e));
                });
        }
        else
        {
            return registry.obsDatabases.values().stream()
                .flatMap(db -> {
                    int dbID = db.getDatabaseID();
                    return db.getObservationStore().getDataStreams().selectEntries(filter, fields)
                        .map(e -> toPublicEntry(dbID, e));
                });
        }
    }


    @Override
    public DataStreamKey add(IDataStreamInfo dsInfo)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }

}
