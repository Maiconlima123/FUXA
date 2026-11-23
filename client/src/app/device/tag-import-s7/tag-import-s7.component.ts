import { Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { Device, Tag, TAG_PREFIX } from '../../_models/device';
import { Utils } from '../../_helpers/utils';

export interface TagImportS7Data {
    device: Device;
    tags: Tag[];
}

interface ImportRow {
    name: string;
    fuxaType: string;
    address: string;
    offsetText: string;
    status: 'new' | 'duplicate' | 'invalid';
    message?: string;
}

// Layout de DB S7 (não otimizado). Alinhamento:
// Bool = bit empacotado; Byte = 1; Int/Word = 2; DInt/DWord/Real = 4 (config).
const TYPE_MAP: { [k: string]: { fuxa: string; kind: string; size: number; align: number } } = {
    bool: { fuxa: 'Bool', kind: 'DBX', size: 0, align: 0 },
    byte: { fuxa: 'Byte', kind: 'DBB', size: 1, align: 1 },
    char: { fuxa: 'Byte', kind: 'DBB', size: 1, align: 1 },
    sbyte: { fuxa: 'Byte', kind: 'DBB', size: 1, align: 1 },
    usint: { fuxa: 'Byte', kind: 'DBB', size: 1, align: 1 },
    int: { fuxa: 'Int', kind: 'DBW', size: 2, align: 2 },
    sint: { fuxa: 'Int', kind: 'DBW', size: 2, align: 2 },
    word: { fuxa: 'Word', kind: 'DBW', size: 2, align: 2 },
    uint: { fuxa: 'Word', kind: 'DBW', size: 2, align: 2 },
    dint: { fuxa: 'DInt', kind: 'DBD', size: 4, align: 4 },
    udint: { fuxa: 'DWord', kind: 'DBD', size: 4, align: 4 },
    dword: { fuxa: 'DWord', kind: 'DBD', size: 4, align: 4 },
    real: { fuxa: 'Real', kind: 'DBD', size: 4, align: 4 },
    lreal: { fuxa: 'Real', kind: 'DBD', size: 8, align: 4 },
};

@Component({
    selector: 'app-tag-import-s7',
    templateUrl: './tag-import-s7.component.html',
    styleUrls: ['./tag-import-s7.component.scss']
})
export class TagImportS7Component {

    dbNumber: number = null;
    baseOffset = 0;
    prefixFilter = '';
    alignDword4 = true;
    fileName = '';
    parseError = '';
    rows: ImportRow[] = [];
    format: 'scl' | 'csv' | 'unknown' = 'unknown';

    existingNames: Set<string>;
    existingAddresses: Set<string>;

    constructor(
        public dialogRef: MatDialogRef<TagImportS7Component>,
        @Inject(MAT_DIALOG_DATA) public data: TagImportS7Data
    ) {
        const tags = (data && data.tags) ? data.tags : [];
        this.existingNames = new Set(tags.map((t) => String(t.name || '').trim().toLowerCase()));
        this.existingAddresses = new Set(tags.map((t) => this.normAddr(t.address)));
    }

    normAddr(addr: string): string {
        return String(addr || '').replace(/\s/g, '').replace(/^%/, '').toUpperCase();
    }

    onFile(ev: any) {
        const file: File = ev.target && ev.target.files ? ev.target.files[0] : null;
        if (!file) { return; }
        this.fileName = file.name;
        const reader = new FileReader();
        reader.onload = () => {
            try {
                this.parse(String(reader.result || ''), file.name);
            } catch (e) {
                this.rows = [];
                this.parseError = (e && e.message) ? e.message : 'Falha ao ler o arquivo.';
            }
        };
        reader.readAsText(file);
        ev.target.value = '';
    }

    parse(text: string, fileName: string) {
        this.parseError = '';
        this.rows = [];
        const clean = text.replace(/\r/g, '');
        if (/DATA_BLOCK/i.test(clean) && /STRUCT/i.test(clean)) {
            this.format = 'scl';
            this.parseScl(clean);
        } else {
            this.format = 'csv';
            this.parseCsv(clean);
        }
        if (!this.rows.length && !this.parseError) {
            this.parseError = 'Nenhuma variável encontrada no arquivo.';
        }
    }

    // ---- SCL (fonte externa do TIA: DATA_BLOCK ... STRUCT ... END_STRUCT) ----
    private parseScl(text: string) {
        const lines = text.split('\n');
        let inStruct = false;
        let byteOffset = this.baseOffset;
        let bitPos = 0;
        const closeBits = () => { if (bitPos > 0) { byteOffset++; bitPos = 0; } };
        const align = (a: number) => { if (a > 1 && byteOffset % a !== 0) { byteOffset += a - (byteOffset % a); } };

        for (const raw of lines) {
            const line = raw.trim();
            if (/^STRUCT\b/i.test(line)) { inStruct = true; continue; }
            if (/^END_STRUCT/i.test(line)) { break; }
            if (!inStruct || !line) { continue; }

            const m = /^(?:"([^"]+)"|([A-Za-z_][\w\-]*))\s*:\s*([A-Za-z][A-Za-z0-9_]*)(\s*\[[^\]]*\])?/.exec(line);
            if (!m) { continue; }
            const name = (m[1] || m[2] || '').trim();
            const s7type = (m[3] || '').trim().toLowerCase();
            if (m[4]) { // array — fora do escopo v1
                this.rows.push({ name, fuxaType: '', address: '', offsetText: '', status: 'invalid', message: 'Array não suportado' });
                continue;
            }
            const def = TYPE_MAP[s7type];
            if (!def) {
                this.rows.push({ name, fuxaType: '', address: '', offsetText: '', status: 'invalid', message: `Tipo '${m[3]}' não suportado` });
                continue;
            }
            let address = '';
            let offsetText = '';
            if (s7type === 'bool') {
                address = `DB${this.dbNumber}.DBX${byteOffset}.${bitPos}`;
                offsetText = `${byteOffset}.${bitPos}`;
                bitPos++;
                if (bitPos > 7) { byteOffset++; bitPos = 0; }
            } else {
                closeBits();
                const a = def.align === 4 && !this.alignDword4 ? 2 : def.align;
                align(a);
                address = `DB${this.dbNumber}.${def.kind}${byteOffset}`;
                offsetText = String(byteOffset);
                byteOffset += def.size;
            }
            this.pushRow(name, def.fuxa, address, offsetText);
        }
    }

    // ---- CSV (tabela de tags do PLC e/ou exportação de DB) ----
    private parseCsv(text: string) {
        const lines = text.split('\n').map((l) => l.trim()).filter((l) => l.length > 0);
        if (!lines.length) { return; }
        const delim = (lines[0].match(/;/g) || []).length >= (lines[0].match(/,/g) || []).length ? ';' : ',';
        const split = (l: string) => l.split(delim).map((c) => c.trim().replace(/^"|"$/g, ''));

        const header = split(lines[0]).map((h) => h.toLowerCase());
        const idxName = header.findIndex((h) => h.includes('name') || h.includes('nome'));
        const idxType = header.findIndex((h) => h.includes('data type') || h.includes('tipo') || h === 'type');
        const idxAddr = header.findIndex((h) => h.includes('address') || h.includes('endere'));
        const idxOffset = header.findIndex((h) => h.includes('offset'));
        const hasHeader = idxName >= 0 && (idxType >= 0 || idxAddr >= 0);

        let byteOffset = this.baseOffset;
        const start = hasHeader ? 1 : 0;
        for (let i = start; i < lines.length; i++) {
            const cols = split(lines[i]);
            const name = (hasHeader ? cols[idxName] : cols[0]) || '';
            if (!name) { continue; }
            const s7type = (hasHeader && idxType >= 0 ? cols[idxType] : cols[1]) || '';
            const def = TYPE_MAP[String(s7type).toLowerCase()];
            let address = '';
            let offsetText = '';
            if (hasHeader && idxAddr >= 0 && cols[idxAddr]) {
                address = this.normAddr(cols[idxAddr]);
            } else {
                let off = hasHeader && idxOffset >= 0 ? cols[idxOffset] : cols[2];
                off = String(off || '').replace(/[^\d.]/g, '');
                const parts = off.split('.');
                const bytePart = parseInt(parts[0], 10);
                if (!isNaN(bytePart)) {
                    if (def && def.kind === 'DBX') {
                        address = `DB${this.dbNumber}.DBX${bytePart}.${parts[1] || '0'}`;
                    } else if (def) {
                        address = `DB${this.dbNumber}.${def.kind}${bytePart}`;
                    }
                    offsetText = off;
                }
            }
            this.pushRow(name, def ? def.fuxa : '', address, offsetText);
        }
    }

    private pushRow(name: string, fuxaType: string, address: string, offsetText: string) {
        const cleanName = String(name).replace(/^"|"$/g, '').trim();
        if (!fuxaType || !address) {
            this.rows.push({ name: cleanName, fuxaType: fuxaType || '', address, offsetText, status: 'invalid', message: 'Tipo/endereço não reconhecido' });
            return;
        }
        if (cleanName.includes('@')) {
            this.rows.push({ name: cleanName, fuxaType, address, offsetText, status: 'invalid', message: "Nome não pode conter '@'" });
            return;
        }
        const isDup = this.existingAddresses.has(this.normAddr(address)) || this.existingNames.has(cleanName.toLowerCase());
        this.rows.push({ name: cleanName, fuxaType, address, offsetText, status: isDup ? 'duplicate' : 'new' });
    }

    get newCount(): number {
        return this.rows.filter((r) => r.status === 'new').length;
    }

    visibleRows(): ImportRow[] {
        const p = this.prefixFilter.trim().toLowerCase();
        return p ? this.rows.filter((r) => r.name.toLowerCase().startsWith(p)) : this.rows;
    }

    add() {
        const p = this.prefixFilter.trim().toLowerCase();
        const selected = this.rows.filter((r) => r.status === 'new' && (!p || r.name.toLowerCase().startsWith(p)));
        const tags: Tag[] = selected.map((r) => {
            const tag = new Tag(Utils.getGUID(TAG_PREFIX));
            tag.name = r.name;
            tag.type = r.fuxaType;
            tag.address = r.address;
            tag.description = '';
            return tag;
        });
        this.dialogRef.close(tags);
    }

    cancel() {
        this.dialogRef.close(null);
    }
}
