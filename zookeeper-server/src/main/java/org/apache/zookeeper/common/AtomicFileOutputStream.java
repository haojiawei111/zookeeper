/**
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
package org.apache.zookeeper.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This code is originally from HDFS, see the similarly named files there
 * in case of bug fixing, history, etc...
 * 此代码最初来自HDFS，请在此处查看类似名称的文件，以防错误修复，历史记录等。
 */

/**
 * A FileOutputStream that has the property that it will only show up at its
 * destination once it has been entirely written and flushed to disk. While
 * being written, it will use a .tmp suffix.
 *
 * When the output stream is closed, it is flushed, fsynced, and will be moved
 * into place, overwriting any file that already exists at that location.
 *
 * <b>NOTE</b>: on Windows platforms, it will not atomically replace the target
 * file - instead the target file is deleted before this one is moved into
 * place.
 *
 * FileOutputStream具有的属性，只有在将其完全写入并刷新到磁盘后，才会显示在其目的地。在编写时，它将使用.tmp后缀。
 *
 * 关闭输出流后，将对其进行刷新，fsync和将其移动到位，从而覆盖该位置上已经存在的任何文件。
 *
 * <b>注意</ b>：在Windows平台上，它不会自动替换目标文件-而是在将目标文件移动到位之前删除目标文件。
 *
 */
public class AtomicFileOutputStream extends FilterOutputStream {
    private static final String TMP_EXTENSION = ".tmp";

    private final static Logger LOG = LoggerFactory.getLogger(AtomicFileOutputStream.class);
    // 目标文件
    private final File origFile;
    // 临时文件
    private final File tmpFile;

    public AtomicFileOutputStream(File f) throws FileNotFoundException {
        // Code unfortunately must be duplicated below since we can't assign
        // anything
        // before calling super
        super(new FileOutputStream(new File(f.getParentFile(), f.getName()
                + TMP_EXTENSION)));
        origFile = f.getAbsoluteFile();
        tmpFile = new File(f.getParentFile(), f.getName() + TMP_EXTENSION)
                .getAbsoluteFile();
    }

    /**
     * The default write method in FilterOutputStream does not call the write
     * method of its underlying input stream with the same arguments. Instead
     * it writes the data byte by byte, override it here to make it more
     * efficient.
     * FilterOutputStream中的默认write方法不会使用相同的参数调用其基础输入流的write方法。相反，它逐字节写入数据，此处将其覆盖以使其更有效率。
     */
    @Override
    public void write(byte b[], int off, int len) throws IOException {
        // 写入临时文件
        out.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
        boolean triedToClose = false, success = false;
        try {
            flush();
            ((FileOutputStream) out).getFD().sync();

            triedToClose = true;
            super.close();
            // 临时文件全部写入完成并安全关闭
            success = true;
        } finally {
            if (success) {
                boolean renamed = tmpFile.renameTo(origFile);
                if (!renamed) {
                    // On windows, renameTo does not replace.
                    // 在Windows上，namedTo不会替换。，所以先删除在替换
                    if (!origFile.delete() || !tmpFile.renameTo(origFile)) {
                        throw new IOException(
                                "Could not rename temporary file " + tmpFile
                                        + " to " + origFile);
                    }
                }
            } else {
                if (!triedToClose) {
                    // If we failed when flushing, try to close it to not leak
                    // an FD
                    IOUtils.closeStream(out);
                }
                // close wasn't successful, try to delete the tmp file
                if (!tmpFile.delete()) {
                    LOG.warn("Unable to delete tmp file " + tmpFile);
                }
            }
        }
    }

    /**
     * Close the atomic file, but do not "commit" the temporary file on top of
     * the destination. This should be used if there is a failure in writing.
     */
    public void abort() {
        try {
            super.close();
        } catch (IOException ioe) {
            // 关闭临时文件有IOException错误
            LOG.warn("Unable to abort file " + tmpFile, ioe);
        }
        // 删除临时文件
        if (!tmpFile.delete()) {
            LOG.warn("Unable to delete tmp file during abort " + tmpFile);
        }
    }
}
