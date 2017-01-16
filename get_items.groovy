/*
##### Script para obtener items de clone de las categorías que se indiquen en un .xls ##########
#####  el modo de uso es $ groovy get_items.groovy archivo_con_categorias false  ################
##### Todos los items seran dumpeados a archivo_con_categorias.csv, si el segundo argumento ####
##### del llamado al script es true, se crean unos csv con los items de cada categoría #########
##### por separado. Es decir MLM12345.csv, MLM12346.csv and so on, iguakmente se crea ##########
##### el archivo items_count.txt con las cantidades de item de cada categoría ##################                                           
*/

@Grapes([
    @Grab('org.apache.poi:poi:3.10.1'),
    @Grab('org.apache.poi:poi-ooxml:3.10.1'),
    @Grab(group = 'net.sf.opencsv', module = 'opencsv', version = '2.3')])

import org.apache.poi.ss.usermodel.WorkbookFactory
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import static org.apache.poi.ss.usermodel.Cell.*

import java.nio.file.Paths
import groovy.json.JsonOutput

import groovy.sql.Sql

import au.com.bytecode.opencsv.*

def categories = []

def writer = new StringWriter()

def all_items = new File(args[0] + "_items.csv")
all_items.write("");
all_items.append "CATEG_ID,ITEM_ID,USER_ID"

def all_count = 0

def count_file = new File("items_count.txt")
count_file.write("");

sql = Sql.newInstance("")

Paths.get(args[0] + '.xls').withInputStream { input ->

  def workbook = WorkbookFactory.create(input)    
  def sheet = workbook.getSheetAt(0)
  
  for (row in sheet.rowIterator()) {

    def rowData = [:]
    for (cell in row.cellIterator()) {
      def value = ''
      
      switch(cell.cellType) {
        case CELL_TYPE_STRING:
          value = cell.stringCellValue
          break
        case CELL_TYPE_STRING:
          value = cell.numericCellValue
          break
        default:
          value = ''                
      }
      
      if(value?.trim()) {
        value = value.split("(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)")

        println "CATEGORIA: ${value[0]}${value[1]}"

        if(args[1] == 'true') {
          def cat = new File(value[0] + value[1] + ".csv")
          cat.write("");
          cat.append "CATEG_ID,ITEM_ID,USER_ID"
        }

        def cat_count = 0

        sql.eachRow("select site_id||categ_id as categ_id,site_id||item_id as item_id,seller_id as user_id from orange.items where categ_id= ? and site_id = ? ",[value[1] , value[0]]) {
          if(it) {
            all_items.append("\r\n" + it['categ_id'] + "," + it['item_id'] + "," + it['user_id'] + "\r\n")
            if(args[1] == 'true')
              cat.append("\r\n" + it['categ_id'] + "," + it['item_id'] + "," + it['user_id'] + "\r\n")
            all_count++
            cat_count++
          }
        }
        sql.eachRow("select site_id||categ_id as categ_id,site_id||ih.item_id as item_id, ih.seller_id as user_id from orange.items_history ih where ih.site_id= ? and ih.status in ('F','A','P','W') and ih.categ_id= ? and ih.auction_stop > sysdate-210 and nvl(ih.DELETED,'nulo') <> 'Y' and not exists (select 1 from orange.items i where ih.item_id = i.parent_item_id)",[value[0] , value[1]]) {
          if(it) {
            all_items.append("\r\n" + it['categ_id'] + "," + it['item_id'] + "," + it['user_id'] + "\r\n")
            if(args[1] == 'true')
              cat.append("\r\n" + it['categ_id'] + "," + it['item_id'] + "," + it['user_id'] + "\r\n")
            all_count++
            cat_count++
          }
        }  
        count_file.append("${value[0]}${value[1]}: ${cat_count}\n")
        rowData << ["${value[0]}${value[1]}" : cat_count]
      }
    }
    categories << rowData
  }
}


count_file.append("\nTOTAL_ITEMS: ${all_count}\n")

categories << ['TOTAL_ITEMS' : all_count]

Paths.get('categories-MLM.json').withWriter { jsonWriter ->
  jsonWriter.write JsonOutput.prettyPrint(JsonOutput.toJson(categories))
}