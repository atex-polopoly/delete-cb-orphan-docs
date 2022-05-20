package com.atex;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ChangeAspectBeanTypeTest {

    @Test
    public void testJson () {

        Map<String, AddKioskEngagement.KioskMapping> testMap = new HashMap<>();
        AddKioskEngagement.KioskMapping map1 = new AddKioskEngagement.KioskMapping();
        map1.userId = "fred";
        map1.kioskId = "kk12345";

        testMap.put("123456", map1);

        AddKioskEngagement.KioskMappingSupplier supplier = new AddKioskEngagement.KioskMappingSupplier(testMap);
        AddKioskEngagement.KioskMapping fred1 = AddKioskEngagement.lookupKioskId(supplier, "123456", "1234567890", "fred");

        assertNotNull (fred1);

        assertEquals("fred", fred1.userId);
        assertEquals("kk12345", fred1.kioskId);
        assertEquals("1234567890", fred1.timestamp);

        JsonObject fred = AddKioskEngagement.getKioskEngagementObject(fred1);

        assertNotNull (fred);

        assertEquals ("kiosk:kk12345", fred.getString("appPk"));
        assertEquals ("atex.dm.polopoly", fred.getString("appType"));
    }

    @Test
    public void testFromFile () throws FileNotFoundException {

        AddKioskEngagement.KioskMappingSupplier supplier = new AddKioskEngagement.KioskMappingSupplier(new File(this.getClass().getClassLoader().getResource("test.file").getFile()), 100000);
        AddKioskEngagement.KioskMapping fred1 = AddKioskEngagement.lookupKioskId(supplier, "c1234567890", "1234567890", "fred");

        assertNotNull (fred1);

        assertEquals("ufred", fred1.userId);
        assertEquals("k1234567890", fred1.kioskId);
        assertEquals("t1234567890", fred1.timestamp);

        JsonObject fred = AddKioskEngagement.getKioskEngagementObject(fred1);

        assertNotNull (fred);

        assertEquals ("kiosk:k1234567890", fred.getString("appPk"));
        assertEquals ("atex.dm.polopoly", fred.getString("appType"));
        assertEquals ("ufred", fred.getString("userName"));
        assertEquals("t1234567890", fred.getString("timestamp"));
        JsonArray attributes = fred.getArray("attributes");
        assertNotNull(attributes);

        assertEquals(3, attributes.size());
    }

}