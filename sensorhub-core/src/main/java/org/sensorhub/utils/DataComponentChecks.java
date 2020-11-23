/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.utils;

import org.vast.data.DataIterator;
import org.vast.swe.SWEUtils;
import org.vast.util.Asserts;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashingOutputStream;
import com.google.common.io.ByteStreams;
import net.opengis.swe.v20.BlockComponent;
import net.opengis.swe.v20.DataComponent;


/**
 * <p>
 * Helper methods to check various aspects of SWE Common component structures
 * </p>
 *
 * @author Alex Robin
 * @since Sep 19, 2020
 */
public class DataComponentChecks
{
    static HashFunction hf = Hashing.goodFastHash(32);
    
    
    /**
     * Check if two data components have compatible structures.<br/>
     * Two components have compatible structures if the component names, types
     * and order are the same at all depth levels.
     * @param rec1
     * @param rec2
     * @return True if structures are compatible, false otherwise
     */
    public static boolean checkStructCompatible(DataComponent rec1, DataComponent rec2)
    {
        Asserts.checkNotNull(rec1, DataComponent.class);
        Asserts.checkNotNull(rec2, DataComponent.class);
        
        var hc1 = getStructCompatibilityHashCode(rec1);
        var hc2 = getStructCompatibilityHashCode(rec2);
        return hc1.equals(hc2);
    }
    
    
    public static HashCode getStructCompatibilityHashCode(DataComponent root)
    {
        var hasher = hf.newHasher();
        
        DataIterator it = new DataIterator(root);
        while (it.hasNext())
        {
            DataComponent c = it.next();
            if (c != root)
                hasher.putUnencodedChars(c.getName());
            hasher.putUnencodedChars(getComponentType(c.getClass()));
            if (c instanceof BlockComponent)
                hasher.putInt(c.getComponentCount());
        }
        
        return hasher.hash();
    }
    
    
    /**
     * Check if two data components are equal to each other.<br/>
     * Two components are equal if their structures are compatible and all metadata is
     * identical, including semantics, labels, descriptions and constraints.
     * @param rec1
     * @param rec2
     * @return True if structures are equal, false otherwise
     */
    public static boolean checkStructEquals(DataComponent rec1, DataComponent rec2)
    {
        Asserts.checkNotNull(rec1, DataComponent.class);
        Asserts.checkNotNull(rec2, DataComponent.class);
        
        var hc1 = getStructEqualsHashCode(rec1);
        var hc2 = getStructEqualsHashCode(rec2);
        return hc1.equals(hc2);
    }
    
    
    public static HashCode getStructEqualsHashCode(DataComponent root)
    {        
        try
        {
            var hashOs = new HashingOutputStream(hf, ByteStreams.nullOutputStream());
            new SWEUtils(SWEUtils.V2_0).writeComponent(hashOs, root, false, false);
            return hashOs.hash();
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Error computing hashcode. Invalid data component");
        }
    }
    
    
    private static String getComponentType(Class<?> c)
    {
        for (Class<?> i: c.getInterfaces())
        {
            if (DataComponent.class.isAssignableFrom(i))
                return i.getSimpleName();
        }
        
        if (c.getSuperclass() != null)
            return getComponentType(c.getSuperclass());
        else
            return null;
    }
}
