/*******************************************************************************
 * Copyright 2024 Rein Spanjer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/

package nl.reinspanjer.kcp.utils;

import java.nio.ByteBuffer;

/**
 * Quick and dirty hex dump methods for troubleshooting.
 */
public class LogUtils {

    private static final String MID_COLUMN = "   ";
    private static final byte MIN_CHAR = 0x20;
    private static final byte MAX_CHAR = 0x7E;

    public static void hexDump(String title, ByteBuffer buffer) {
        int pos = buffer.position();
        int rem = buffer.remaining();
        byte[] bytes = new byte[rem];
        buffer.get(bytes);
        hexDump(title, bytes);
        buffer.position(pos);
    }

    public static void hexDump(String title, byte[] buffer) {

        if (buffer == null) {
            return;
        }
        if (title != null) {
            title = String.format("%s (buffer.length=%d %04X bytes)", title, buffer.length,
                    buffer.length);
            System.out.println(title);
        }
        StringBuilder hex = new StringBuilder();
        StringBuilder chars = new StringBuilder();
        int i = 0;
        for (i = 0; i < buffer.length; i++) {

            if ((i > 0) && (i % 16 == 0)) {
                hex.append(MID_COLUMN);
                hex.append(chars);
                hex.append('\n');
                chars = new StringBuilder();
            }
            byte b = buffer[i];
            hex.append(String.format("%02X ", b));
            if (b >= MIN_CHAR && b < MAX_CHAR) {
                chars.append((char) b);
            } else {
                chars.append('.');
            }
        }

        // loop over. add remainders
        if (chars.length() > 0) {
            if (i % 16 != 0) {
                for (int j = i % 16; j < 16; j++) {
                    hex.append("   ");
                }
            }
            hex.append(MID_COLUMN);
            hex.append(chars);
            hex.append('\n');
        }
        // for now, write to stdout
        System.out.println(hex);
    }


}
