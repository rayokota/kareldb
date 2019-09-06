/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kareldb.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvroUtils {

    private static final SchemaValidator BACKWARD_TRANSITIVE_VALIDATOR =
        new SchemaValidatorBuilder().canReadStrategy().validateAll();

    /**
     * Check the compatibility between the new schema and the specified schemas
     *
     * @param newSchema       The new schema
     * @param previousSchemas Full schema history in chronological order
     */
    public static void checkCompatibility(Schema newSchema, List<Schema> previousSchemas)
        throws SchemaValidationException {
        List<Schema> previousSchemasCopy = new ArrayList<>(previousSchemas);
        BACKWARD_TRANSITIVE_VALIDATOR.validate(newSchema, previousSchemasCopy);
    }

    public static Schema parseSchema(File file) throws IOException {
        try {
            Schema.Parser parser = new Schema.Parser();
            return parser.parse(file);
        } catch (SchemaParseException e) {
            return null;
        }
    }

    public static Schema parseSchema(String schemaString) {
        try {
            Schema.Parser parser = new Schema.Parser();
            return parser.parse(schemaString);
        } catch (SchemaParseException e) {
            return null;
        }
    }
}
