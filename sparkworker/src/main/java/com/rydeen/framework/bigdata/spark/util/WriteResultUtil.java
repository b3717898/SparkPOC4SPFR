package com.rydeen.framework.bigdata.spark.util;

import com.yum.boh.core.util.FileUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriteResultUtil {
    public synchronized static void writeResult(File f, String content) throws Exception {
        boolean flag = false;
        FileWriter fsStream = null;
        BufferedWriter out = null;

        try {
            File folder = new File(f.getParent());
            if (!folder.exists()) {
                folder.mkdirs();
            }
            fsStream = new FileWriter(f,true);
            out = new BufferedWriter(fsStream);
            out.write(content);
            out.close();
            flag = true;
        } catch (Exception var18) {
            throw var18;
        } finally {
            if (fsStream != null) {
                try {
                    fsStream.close();
                } catch (IOException var17) {
                    var17.printStackTrace();

                }
            }

            if (out != null) {
                try {
                    out.close();
                } catch (IOException var16) {
                    var16.printStackTrace();

                }
            }

        }
    }
}
