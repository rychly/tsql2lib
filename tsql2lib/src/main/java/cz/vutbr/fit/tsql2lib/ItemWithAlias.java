/**
 * Processor of TSQL2 on a Relational Database System
 *
 * LICENSE
 *
 * This source file is subject to the new BSD license that is bundled
 * with this package in the file LICENSE.
 * It is also available through the world-wide-web at this URL:
 * http://www.opensource.org/licenses/bsd-license.php
 *
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 * @license http://www.opensource.org/licenses/bsd-license.php New BSD License
 */
package cz.vutbr.fit.tsql2lib;

/**
 * Class for storing object from SQL statement that can have AS part (alias).
 * This class is used when generating SELECT statements to store select items
 * and table references with their aliases.
 *
 * @author Jiri Tomek <katulus@volny.cz>
 * @copyright Copyright (c) 2008-2009 Jiri Tomek <katulus@volny.cz>
 */
public class ItemWithAlias {

    /**
     * Original item name
     */
    private String _item = "";
    /**
     * Optional alias for item
     */
    private String _alias = "";

    /**
     * Create new item with alias
     *
     * @param item Original item name
     * @param alias Alias name for item
     */
    public ItemWithAlias(String item, String alias) {
        _item = item;
        _alias = alias;
    }

    /**
     * Create new item without alias
     *
     * @param item Item name
     */
    public ItemWithAlias(String item) {
        _item = item;
    }

    /**
     * Create new empty item
     */
    public ItemWithAlias() {
    }

    /**
     * Get original item name
     *
     * @return Original item name
     */
    public String getItem() {
        return _item;
    }

    /**
     * Set original item name
     *
     * @param item Original item name
     */
    public void setItem(String item) {
        this._item = item;
    }

    /**
     * Get item alias
     *
     * @return Item alias. If item has no alias set, this returns original item
     * name.
     */
    public String getAlias() {
        if (_alias.length() > 0) {
            return _alias;
        } else {
            return _item;
        }
    }

    /**
     * Set item alias
     *
     * @param alias Item alias
     */
    public void setAlias(String alias) {
        this._alias = alias;
    }

    /**
     * Check if item has alias
     *
     * @return True if item has alias, false otherwise
     */
    public boolean hasAlias() {
        return (_alias.length() > 0);
    }
}
