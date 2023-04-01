/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.serializers;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.json.simple.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectSerializer
{
    private static final Logger logger = LoggerFactory.getLogger(ObjectSerializer.class);

    public ObjectSerializer()
    {
    }

    public static void serialize(Object obj)
    {
        logger.debug("serialize " + obj.getClass().toString());
        logger.debug("is null " + (obj == null));
        logger.debug("value " + (obj));
        JSONObject jsonObject = serializeRecursion(obj, 0, new HashSet<String>());
        logger.debug(jsonObject.toJSONString());
    }

    public static JSONObject serializeRecursion(Object obj, int depth, Set<String> visitSet)
    {
        if (obj == null)
        {
            logger.debug("return null!");
            return null;
        }
        JSONObject struct = new JSONObject();
        {
            String hash = null;

            try
            {
                hash = obj.getClass().getName();
                hash += ";@" + obj.hashCode();
            }
            catch (Exception e)
            {
                logger.debug(Arrays.toString(e.getStackTrace()));
            }
            struct.put("signature_hash", hash);
            if (visitSet.contains(hash))
            {
                struct.put("type", "Virtual Object");
                return struct;
            }
            else
            {
                visitSet.add(hash);
            }

            // TODO skip enum
            if (obj instanceof Object)
            {
                logger.debug(hash + " is a reference");
            }
            else
            {
                logger.debug(hash + " is not a reference");
            }
            if (hash.startsWith("org.apache"))
            {
                try
                {
                    JSONObject subStruct = new JSONObject();
                    Field[] fields = obj.getClass().getDeclaredFields();
                    for (Field field : fields)
                    {
                        field.setAccessible(true);
                        Object value = field.get(obj);
                        if (value == null)
                        {
                            continue;
                        }
                        subStruct = serializeRecursion(value, depth + 1, visitSet);
                        struct.put("field::" + field.getName(), subStruct);
                    }
                }
                catch (IllegalArgumentException | IllegalAccessException e)
                {
                    logger.debug(e.getMessage(), e);
                }
            }
            else if (obj instanceof Iterable)
            {
                JSONArray subStruct = new JSONArray();
                Iterator<Object> iterator = ((Iterable<Object>)obj).iterator();
                int index = 0;
                struct.put("type", "Iterable");
                while (iterator.hasNext())
                {
                    Object element = iterator.next();
                    JSONObject elementStruct = serializeRecursion(element, depth + 1, visitSet);
                    subStruct.add(elementStruct);
                    ++index;
                }
                struct.put("Iterable", subStruct);
            }
            else if (obj instanceof Map)
            {
                struct.put("type", "Map");
                JSONArray subStruct = new JSONArray();
                int index = 0;
                for (Map.Entry<Object, Object> entry : ((Map<Object, Object>)obj).entrySet())
                {
                    JSONObject elementStruct = new JSONObject();
                    JSONObject keyStruct = serializeRecursion(entry.getKey(), depth + 1, visitSet);
                    JSONObject valueStruct = serializeRecursion(entry.getValue(), depth + 1, visitSet);
                    elementStruct.put("key", keyStruct);
                    elementStruct.put("value", valueStruct);
                    subStruct.add(elementStruct);
                }
                struct.put("Map", subStruct);
            }
            else
            { // primitives
                struct.put("value", obj.toString());
            }
        }
        return struct;
    }
}
